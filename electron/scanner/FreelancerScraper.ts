import { EventEmitter } from 'node:events';
import { chromium, type Browser, type BrowserContext } from 'playwright';
import { getFreelasDir, writeScrapedOpportunity } from '../services/ExecutionStorage';
import { ActivityLogger } from '../services/ActivityLogger';
import { getDb } from '../db/client';
import * as schema from '../db/schema';
import type {
  ScrapperOptions,
  ScrapperJob,
  ScrapperEvent,
  ScrapperResult,
} from './WorkanaScraper';

// Reaproveita os TIPOS públicos do WorkanaScraper (mesmo contrato de IPC/UI),
// sem importar nenhuma lógica dele. A UI e os handlers tratam os dois scrapers
// de forma intercambiável.
export type {
  ScrapperOptions,
  ScrapperJob,
  ScrapperEvent,
  ScrapperResult,
} from './WorkanaScraper';

/** Origem do site — as URLs das vagas na listagem vêm relativas (/projects/…). */
const FREELANCER_ORIGIN = 'https://www.freelancer.com';

/** Objeto bruto devolvido pelo extrator que roda no contexto da página. */
interface RawScraped {
  title: string;
  url: string;
  description: string;
  skills: string[];
  budgetText: string;
  postedText: string;
  proposalsText: string;
  strategy: string;
}

const FREELANCER_UA =
  'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36';

/** System prompt padrão do scraper do Freelancer (configurável em Settings → Playwright). */
export const DEFAULT_FREELANCER_SYSTEM_PROMPT =
  'Você é um agente de raspagem de vagas do Freelancer.com. Extraia título, descrição, ' +
  'orçamento, skills e URL de cada projeto com fidelidade ao original, sem inventar dados. ' +
  'Normalize apenas espaços e quebras de linha; preserve o idioma original do anúncio.';

type BrowserChannelPref = 'auto' | 'chromium' | 'msedge' | 'chrome';

/** Config efetiva do Playwright, resolvida a partir das settings (Settings → Playwright). */
interface PlaywrightConfig {
  headless: boolean;
  channel: BrowserChannelPref;
  userAgent: string;
  locale: string;
  viewportW: number;
  viewportH: number;
  navTimeout: number;
  selectorTimeout: number;
  networkidleTimeout: number;
  pagePause: number;
  delayMin: number;
  delayMax: number;
  blockResources: boolean;
  maxPages: number;
  systemPrompt: string;
}

/**
 * Lê as configurações do Playwright da tabela `settings` (chaves `playwright.*`),
 * caindo nos defaults quando ausentes. As chaves são compartilhadas com o
 * WorkanaScraper — só o system prompt tem default próprio do Freelancer.
 */
function readPlaywrightConfig(): PlaywrightConfig {
  let map = new Map<string, string>();
  try {
    const rows = getDb().select().from(schema.settings).all();
    map = new Map(rows.map((r) => [r.key, r.value ?? '']));
  } catch {
    /* sem DB acessível → defaults */
  }
  const str = (k: string, d: string) => {
    const v = map.get(k);
    return v == null || v === '' ? d : v;
  };
  const num = (k: string, d: number) => {
    const v = map.get(k);
    if (v == null || v === '') return d;
    const n = Number(v);
    return Number.isFinite(n) ? n : d;
  };
  const bool = (k: string, d: boolean) => {
    const v = map.get(k);
    return v == null || v === '' ? d : v === 'true';
  };
  const chRaw = str('playwright.browser_channel', 'auto');
  const channel: BrowserChannelPref = (
    ['auto', 'chromium', 'msedge', 'chrome'].includes(chRaw) ? chRaw : 'auto'
  ) as BrowserChannelPref;
  return {
    headless: bool('playwright.headless', true),
    channel,
    userAgent: str('playwright.user_agent', FREELANCER_UA),
    // Freelancer.com é majoritariamente en; en-US evita a UI cair em outro idioma.
    locale: str('playwright.locale', 'en-US'),
    viewportW: Math.max(320, num('playwright.viewport_width', 1366)),
    viewportH: Math.max(320, num('playwright.viewport_height', 900)),
    navTimeout: Math.max(1000, num('playwright.nav_timeout_ms', 45_000)),
    selectorTimeout: Math.max(1000, num('playwright.selector_timeout_ms', 15_000)),
    networkidleTimeout: Math.max(0, num('playwright.networkidle_timeout_ms', 8_000)),
    pagePause: Math.max(0, num('playwright.page_pause_ms', 700)),
    delayMin: Math.max(0, num('playwright.delay_min_ms', 2_000)),
    delayMax: Math.max(0, num('playwright.delay_max_ms', 5_000)),
    blockResources: bool('playwright.block_resources', false),
    maxPages: Math.max(1, Math.min(200, num('playwright.max_pages', 50))),
    systemPrompt: str('playwright.system_prompt', DEFAULT_FREELANCER_SYSTEM_PROMPT),
  };
}

/**
 * Serviço de raspagem do Freelancer.com. Segue o mesmo padrão do WorkanaScraper
 * (Chromium real via Playwright, eventos de progresso em streaming, gravação no
 * formato JSON de `{workspace}/freelas/`), com três diferenças estruturais do
 * site:
 *  1. paginação por PATH (`/jobs/2`, `/jobs/3`) em vez de query `?page=N`;
 *  2. links das vagas RELATIVOS (`/projects/…`) → resolvidos contra a origem;
 *  3. a página de detalhe é um SPA Angular que NÃO expõe a descrição completa a
 *     visitantes deslogados — por isso raspamos tudo direto dos cards da
 *     listagem (single-pass), sem uma segunda fase abrindo cada vaga.
 */
class FreelancerScraperImpl extends EventEmitter {
  private running = false;
  private cancelled = false;
  private browser: Browser | null = null;

  isRunning() {
    return this.running;
  }

  private emitEvent(evt: ScrapperEvent) {
    this.emit('event', evt);
  }

  private log(message: string, level: ScrapperEvent['level'] = 'info') {
    this.emitEvent({ type: 'log', message, level });
  }

  /** Inicia a raspagem. Resolve com o resultado final; progresso vem por eventos. */
  async start(opts: ScrapperOptions): Promise<ScrapperResult> {
    if (this.running) {
      const msg = 'Já existe uma raspagem em andamento.';
      this.emitEvent({ type: 'error', level: 'error', error: msg });
      return { ok: false, totalJobs: 0, savedJobs: 0, dir: '', error: msg };
    }

    // Valida URL / domínio (equivalente ao check workana.com do WorkanaScraper).
    let parsed: URL;
    try {
      parsed = new URL(opts.url);
    } catch {
      const msg = `URL inválida: "${opts.url}".`;
      this.emitEvent({ type: 'error', level: 'error', error: msg });
      return { ok: false, totalJobs: 0, savedJobs: 0, dir: '', error: msg };
    }
    if (!/(^|\.)freelancer\.com$/i.test(parsed.hostname)) {
      const msg = `A URL precisa ser do Freelancer (freelancer.com). Recebido: ${parsed.hostname}`;
      this.emitEvent({ type: 'error', level: 'error', error: msg });
      return { ok: false, totalJobs: 0, savedJobs: 0, dir: '', error: msg };
    }

    const cfg = readPlaywrightConfig();
    const totalPages = Math.max(1, Math.min(cfg.maxPages, Math.floor(opts.pages) || 1));
    const headless = opts.headless ?? cfg.headless;
    const clampDelay = (v: number) => Math.max(0, Math.min(60_000, Math.floor(v)));
    const delayMin = clampDelay(opts.delayMinMs ?? cfg.delayMin);
    const delayMax = Math.max(delayMin, clampDelay(opts.delayMaxMs ?? cfg.delayMax));

    // Resolve a pasta freelas/ cedo: falha já com mensagem clara se o workspace
    // não estiver configurado (antes de abrir o navegador).
    let dir: string;
    try {
      dir = getFreelasDir();
    } catch (e) {
      const msg = (e as Error).message;
      this.emitEvent({ type: 'error', level: 'error', error: msg });
      return { ok: false, totalJobs: 0, savedJobs: 0, dir: '', error: msg };
    }

    this.running = true;
    this.cancelled = false;
    let context: BrowserContext | null = null;
    let totalJobs = 0;
    let savedJobs = 0;
    const seenUrls = new Set<string>();
    const collected: Array<{ raw: RawScraped; canonical: string; page: number }> = [];

    this.emitEvent({ type: 'start', totalPages, dir });
    this.log(`🚀 Iniciando raspagem do Freelancer · ${totalPages} página(s)`, 'info');
    if (cfg.systemPrompt.trim()) {
      this.log(`📋 System prompt carregado (${cfg.systemPrompt.trim().length} caracteres)`, 'info');
    }

    try {
      this.log(`🤖 Abrindo navegador (${headless ? 'headless' : 'visível'} · ${cfg.channel})…`, 'info');
      this.browser = await this.launchBrowser(headless, cfg.channel);
      context = await this.browser.newContext({
        userAgent: cfg.userAgent,
        locale: cfg.locale,
        viewport: { width: cfg.viewportW, height: cfg.viewportH },
      });

      // Bloqueia imagens/fontes/mídia pra acelerar (não afeta a extração de texto).
      if (cfg.blockResources) {
        this.log('🚫 Bloqueando imagens/fontes/mídia (modo rápido)…', 'info');
        await context.route('**/*', (route) => {
          const type = route.request().resourceType();
          if (type === 'image' || type === 'font' || type === 'media') route.abort().catch(() => undefined);
          else route.continue().catch(() => undefined);
        });
      }

      const page = await context.newPage();

      // ── Varre as listagens e extrai cada vaga direto do card (single-pass) ──
      // A descrição completa não é acessível a deslogados na página de detalhe
      // (SPA Angular), então o card é a fonte de verdade — igual em cada página.
      for (let i = 1; i <= totalPages; i++) {
        if (this.cancelled) break;

        const pageUrl = this.buildPageUrl(parsed, i);
        this.log(`📄 Carregando página ${i}/${totalPages} → ${pageUrl}`, 'info');

        try {
          await page.goto(pageUrl, { waitUntil: 'domcontentloaded', timeout: cfg.navTimeout });
          // Espera os cards renderizarem.
          await page
            .waitForSelector('.JobSearchCard-item, a[href*="/projects/"]', { timeout: cfg.selectorTimeout })
            .catch(() => undefined);
          if (cfg.networkidleTimeout > 0) {
            await page.waitForLoadState('networkidle', { timeout: cfg.networkidleTimeout }).catch(() => undefined);
          }
        } catch (e) {
          this.log(`⚠️ Falha ao carregar a página ${i}: ${(e as Error).message}`, 'warn');
          continue;
        }

        if (this.cancelled) break;

        const raws = await page.evaluate(extractJobsInPage).catch((e) => {
          this.log(`⚠️ Erro extraindo a página ${i}: ${(e as Error).message}`, 'warn');
          return [] as RawScraped[];
        });

        let novas = 0;
        for (const raw of raws) {
          const canonical = canonicalUrl(raw.url);
          if (!canonical || seenUrls.has(canonical)) continue;
          seenUrls.add(canonical);
          collected.push({ raw, canonical, page: i });
          novas++;
        }
        totalJobs = collected.length;
        const strategy = raws[0]?.strategy ?? 'nenhuma';
        this.emitEvent({
          type: 'page',
          page: i,
          totalPages,
          jobsOnPage: novas,
          totalJobs,
          message: `🔎 Página ${i}: ${raws.length} vaga(s) detectada(s), ${novas} nova(s) (estratégia: ${strategy})`,
          level: raws.length > 0 ? 'info' : 'warn',
        });

        // Pausa curta entre páginas de listagem (cortesia / configurável).
        if (i < totalPages && !this.cancelled && cfg.pagePause > 0) {
          await page.waitForTimeout(cfg.pagePause).catch(() => undefined);
        }
      }

      // ── Grava cada vaga coletada no formato JSON de freelas/ ──
      const total = collected.length;
      if (!this.cancelled) {
        this.log(`🔍 ${total} vaga(s) coletada(s). Gravando cada uma…`, 'info');
      }

      for (let k = 0; k < total; k++) {
        if (this.cancelled) break;
        const { raw, canonical, page: srcPage } = collected[k];

        const budget = parseBudget(raw.budgetText);
        const id = stableId(canonical);
        const nowIso = new Date().toISOString();
        const payload = {
          id,
          title: raw.title,
          description: raw.description,
          source_site_id: null as number | null,
          source_url: canonical,
          budget_min: budget.min,
          budget_max: budget.max,
          currency: budget.currency,
          match_score: 0,
          status: 'new',
          detected_tags: raw.skills,
          found_at: nowIso,
          created_at: nowIso,
          updated_at: nowIso,
          // Metadados extras do scraping (ignorados pelo leitor de freelas/).
          platform: 'freelancer',
          posted_at_text: raw.postedText || null,
          budget_text: raw.budgetText || null,
          proposals_text: raw.proposalsText || null,
          page: srcPage,
          scraped_at: nowIso,
        };

        const jobSummary: ScrapperJob = {
          title: raw.title,
          url: canonical,
          budget: raw.budgetText || formatBudget(budget),
          tags: raw.skills,
        };

        try {
          const filePath = writeScrapedOpportunity(payload);
          savedJobs++;
          this.emitEvent({
            type: 'job',
            job: jobSummary,
            totalJobs: total,
            savedJobs,
            filePath,
            level: 'success',
          });
        } catch (e) {
          this.log(`❌ Falha ao gravar "${raw.title}": ${(e as Error).message}`, 'error');
        }

        // Sleep ALEATÓRIO entre min e max a cada vaga (anti-spam).
        if (k < total - 1 && !this.cancelled && delayMax > 0) {
          const wait =
            delayMin >= delayMax
              ? delayMin
              : delayMin + Math.floor(Math.random() * (delayMax - delayMin + 1));
          this.log(`💤 Aguardando ${(wait / 1000).toFixed(1)}s antes da próxima vaga…`, 'info');
          await page.waitForTimeout(wait).catch(() => undefined);
        }
      }

      if (this.cancelled) {
        this.log(`🛑 Raspagem cancelada. ${savedJobs} vaga(s) salva(s).`, 'warn');
        ActivityLogger.log({
          type: 'scan',
          title: 'Raspagem do Freelancer cancelada',
          description: `${savedJobs} vaga(s) salva(s) em freelas/`,
          metadata: { totalJobs, savedJobs, cancelled: true },
        });
        this.emitEvent({ type: 'cancelled', totalJobs, savedJobs, dir });
        return { ok: false, totalJobs, savedJobs, dir, error: 'cancelado' };
      }

      this.log(`🎉 Raspagem concluída · ${savedJobs} vaga(s) salva(s) em freelas/`, 'success');
      ActivityLogger.log({
        type: 'scan',
        title: 'Raspagem do Freelancer concluída',
        description: `${savedJobs} vaga(s) salva(s) em freelas/ (de ${totalJobs} detectada(s))`,
        metadata: { totalJobs, savedJobs, pages: totalPages },
      });
      this.emitEvent({ type: 'done', totalJobs, savedJobs, dir });
      return { ok: true, totalJobs, savedJobs, dir };
    } catch (e) {
      const msg = (e as Error).message;
      this.log(`❌ Erro fatal na raspagem: ${msg}`, 'error');
      ActivityLogger.log({
        type: 'error',
        title: 'Erro na raspagem do Freelancer',
        description: msg,
        metadata: { totalJobs, savedJobs },
      });
      this.emitEvent({ type: 'error', level: 'error', error: msg, totalJobs, savedJobs, dir });
      return { ok: false, totalJobs, savedJobs, dir, error: msg };
    } finally {
      try {
        await context?.close();
      } catch {
        /* ignore */
      }
      try {
        await this.browser?.close();
      } catch {
        /* ignore */
      }
      this.browser = null;
      this.running = false;
    }
  }

  /** Pede o cancelamento da raspagem em andamento (fecha o navegador). */
  cancel(): boolean {
    if (!this.running) return false;
    this.cancelled = true;
    this.log('🛑 Cancelando raspagem…', 'warn');
    this.browser?.close().catch(() => undefined);
    return true;
  }

  /**
   * Lança o navegador conforme a preferência de canal. `auto` tenta o Chromium
   * do Playwright e cai para Edge/Chrome do sistema; um canal específico tenta
   * ele primeiro e usa os demais como fallback.
   */
  private async launchBrowser(headless: boolean, channel: BrowserChannelPref): Promise<Browser> {
    const args = ['--disable-blink-features=AutomationControlled', '--no-sandbox'];
    const defs: Record<'chromium' | 'msedge' | 'chrome', { label: string; launch: () => Promise<Browser> }> = {
      chromium: { label: 'Chromium', launch: () => chromium.launch({ headless, args }) },
      msedge: { label: 'Edge', launch: () => chromium.launch({ headless, channel: 'msedge', args }) },
      chrome: { label: 'Chrome', launch: () => chromium.launch({ headless, channel: 'chrome', args }) },
    };
    const base: Array<'chromium' | 'msedge' | 'chrome'> = ['chromium', 'msedge', 'chrome'];
    const order = channel === 'auto' ? base : [channel, ...base.filter((c) => c !== channel)];

    let lastErr: unknown;
    for (const key of order) {
      const a = defs[key];
      try {
        return await a.launch();
      } catch (e) {
        lastErr = e;
        this.log(`⚠️ Navegador "${a.label}" indisponível, tentando próximo…`, 'warn');
      }
    }
    throw new Error(
      `Não foi possível abrir um navegador (Chromium/Edge/Chrome): ${(lastErr as Error)?.message ?? lastErr}`,
    );
  }

  /**
   * Monta a URL da página `page`. O Freelancer pagina por PATH: a página 2 de
   * `/jobs` é `/jobs/2`. A página 1 usa a URL base como está. Se o path já
   * terminar num número de página, ele é substituído (evita `/jobs/2/3`).
   */
  private buildPageUrl(base: URL, page: number): string {
    const u = new URL(base.toString());
    // Remove barra final e um eventual sufixo numérico de página já presente.
    const cleanPath = u.pathname.replace(/\/+$/, '').replace(/\/\d+$/, '');
    u.pathname = page <= 1 ? cleanPath : `${cleanPath}/${page}`;
    return u.toString();
  }
}

/* ───────────────────────── helpers (rodam no Node) ───────────────────────── */

/** Normaliza a URL da vaga: remove query/hash e barra final. */
function canonicalUrl(raw: string): string | null {
  if (!raw) return null;
  try {
    const u = new URL(raw);
    u.search = '';
    u.hash = '';
    return u.toString().replace(/\/$/, '');
  } catch {
    return null;
  }
}

/** Inteiro positivo estável derivado da URL (hash tipo Java string). */
function stableId(url: string): number {
  let h = 0;
  for (let i = 0; i < url.length; i++) {
    h = (Math.imul(h, 31) + url.charCodeAt(i)) | 0;
  }
  return Math.abs(h) || 1;
}

/** Converte um token numérico ("1,000", "100") em inteiro. */
function normalizeNumber(s: string): number | null {
  const digits = s.replace(/[^\d]/g, '');
  if (!digits) return null;
  const n = parseInt(digits, 10);
  return Number.isFinite(n) ? n : null;
}

/**
 * Interpreta o texto de orçamento do Freelancer e devolve faixa + moeda. Mesma
 * lógica reaproveitada do WorkanaScraper (o formato bate): extrai os tokens
 * numéricos do bloco de preço. No Freelancer os valores vêm em USD por padrão
 * ("$"), e cobrem: "$16 - $131" (faixa), "$389 Average bid" (valor único /
 * média de lances), "$19 / hr Average bid" (por hora).
 */
function parseBudget(text: string): { min: number | null; max: number | null; currency: string } {
  const t = (text || '').trim();
  const currency = /R\$/.test(t) ? 'BRL' : /US\$|USD|\$/.test(t) ? 'USD' : 'USD';
  if (!t) return { min: null, max: null, currency };
  const tokens = t.match(/\d[\d.,]*/g) ?? [];
  const nums = tokens.map(normalizeNumber).filter((n): n is number => n != null);
  if (nums.length === 0) return { min: null, max: null, currency };
  if (/menos de|less than|up to|at[eé]\b/i.test(t)) return { min: null, max: nums[0], currency };
  if (/mais de|more than|acima/i.test(t)) return { min: nums[0], max: null, currency };
  if (nums.length >= 2) {
    const a = nums[0];
    const b = nums[1];
    return { min: Math.min(a, b), max: Math.max(a, b), currency };
  }
  return { min: nums[0], max: nums[0], currency };
}

/** Texto curto de orçamento para o log quando não há `budgetText` original. */
function formatBudget(b: { min: number | null; max: number | null; currency: string }): string | null {
  if (b.min == null && b.max == null) return null;
  const sym = b.currency === 'BRL' ? 'R$' : 'US$';
  if (b.min != null && b.max != null) return `${sym} ${b.min} – ${sym} ${b.max}`;
  if (b.max != null) return `até ${sym} ${b.max}`;
  return `a partir de ${sym} ${b.min}`;
}

/* ─────────────── extrator que roda DENTRO da página (browser) ─────────────── */
/**
 * IMPORTANTE: esta função é serializada e executada no contexto do navegador
 * (page.evaluate). Não pode referenciar nada do escopo Node — tudo é inline.
 * Como o tsconfig do main não inclui a lib DOM, `document` e os elementos são
 * tratados como `any` (os tipos não importam: o código roda no Chromium).
 * Estratégia em camadas: usa os seletores conhecidos do card `.JobSearchCard`
 * e, se a lista vier vazia, cai num fallback genérico via links `/projects/`.
 */
function extractJobsInPage(): RawScraped[] {
  /* eslint-disable @typescript-eslint/no-explicit-any */
  const doc: any = (globalThis as any).document;
  const ORIGIN = 'https://www.freelancer.com';
  const clean = (s: string | null | undefined) => (s || '').replace(/\s+/g, ' ').trim();
  // Resolve href relativo (/projects/…) contra a origem do Freelancer.
  const absUrl = (href: string | null | undefined): string => {
    const h = href || '';
    if (!h) return '';
    try {
      return new URL(h, ORIGIN).toString();
    } catch {
      return h;
    }
  };

  const extractFromCard = (card: any, titleAnchor: any, strategy: string): RawScraped => {
    // O título do card NÃO vem truncado no Freelancer — usa o texto do link.
    const title =
      clean(titleAnchor.textContent) ||
      clean(card.querySelector('h1,h2,h3')?.textContent);

    // Descrição resumida do card (truncada com "…" pelo site).
    const descEl =
      card.querySelector('.JobSearchCard-primary-description') ||
      card.querySelector('[class*="description"]') ||
      card.querySelector('p');
    let description = clean(descEl?.textContent);
    if (!description) {
      const full = clean(card.textContent);
      description = full.replace(title, '').trim();
    }
    if (description.length > 1500) description = description.slice(0, 1500) + '…';

    // Skills/tags: chips `.JobSearchCard-primary-tagsLink`.
    const skillEls: any[] = Array.from(
      card.querySelectorAll('.JobSearchCard-primary-tagsLink, .JobSearchCard-primary-tags a'),
    );
    const skills: string[] = Array.from(
      new Set(
        skillEls
          .map((el: any) => clean(el.textContent))
          .filter((s: string) => s.length > 0 && s.length < 40),
      ),
    ).slice(0, 20);

    // Orçamento: bloco de preço do card. Contém a faixa "$16 - $131" ou a média
    // de lances "$389 Average bid" / por hora "$19 / hr Average bid".
    const budgetEl =
      card.querySelector('.JobSearchCard-primary-price') ||
      card.querySelector('.JobSearchCard-secondary-price') ||
      card.querySelector('[class*="price" i]');
    const budgetText = clean(budgetEl?.textContent);

    // Contagem de propostas: "18 bids".
    const proposalsText = clean(
      card.querySelector('.JobSearchCard-secondary-entry, [class*="secondary-entry"]')?.textContent,
    );

    // "Publicado"/prazo: "6 days left".
    const postedText = clean(
      card.querySelector('.JobSearchCard-primary-heading-days, [class*="heading-days"]')?.textContent,
    );

    return {
      title,
      url: absUrl(titleAnchor.getAttribute?.('href') || titleAnchor.href),
      description,
      skills,
      budgetText,
      postedText,
      proposalsText,
      strategy,
    };
  };

  // Projetos privados/em concurso aparecem com o link apontando pra /login
  // ("Please Sign Up or Login to see details.") — não são vagas acionáveis, então
  // só aceitamos cards cuja URL resolvida seja de fato uma página /projects/.
  const isProjectUrl = (u: string) => /(^|\.)freelancer\.com\/projects\//i.test(u);

  // ── Estratégia 1: seletores conhecidos do card do Freelancer ──
  const knownCards: any[] = Array.from(doc.querySelectorAll('.JobSearchCard-item'));
  const out: RawScraped[] = [];
  if (knownCards.length > 0) {
    for (const card of knownCards) {
      // Prioriza um link /projects/ real; o heading-link pode ser um /login.
      const a =
        card.querySelector('.JobSearchCard-primary-heading-link[href*="/projects/"]') ||
        card.querySelector('a[href*="/projects/"]') ||
        card.querySelector('.JobSearchCard-primary-heading-link');
      if (!a) continue;
      const job = extractFromCard(card, a, 'JobSearchCard-item');
      if (job.title && isProjectUrl(job.url)) out.push(job);
    }
    if (out.length > 0) return out;
  }

  // ── Estratégia 2: fallback genérico via links /projects/ ──
  const anchors: any[] = Array.from(doc.querySelectorAll('a[href*="/projects/"]'));
  // Mantém, por href, a âncora com mais texto (provável título).
  const byHref = new Map<string, any>();
  for (const a of anchors) {
    const text = clean(a.textContent);
    if (text.length < 8) continue;
    const key = String(a.href).split('?')[0].split('#')[0];
    const prev = byHref.get(key);
    if (!prev || clean(prev.textContent).length < text.length) byHref.set(key, a);
  }

  for (const [, a] of byHref) {
    // Sobe até o ancestral que isola UM card (contém só este link de projeto).
    let chosen: any = a.parentElement ?? a;
    let el: any = a.parentElement;
    for (let hop = 0; hop < 6 && el; hop++) {
      const jobLinks = new Set(
        (Array.from(el.querySelectorAll('a[href*="/projects/"]')) as any[]).map(
          (x) => String(x.href).split('?')[0],
        ),
      );
      if (jobLinks.size > 1) break; // passou do card; ancestral anterior é o melhor
      chosen = el;
      el = el.parentElement;
    }
    const job = extractFromCard(chosen, a, 'fallback-link');
    if (job.title) out.push(job);
  }

  return out;
  /* eslint-enable @typescript-eslint/no-explicit-any */
}

export const FreelancerScraper = new FreelancerScraperImpl();
