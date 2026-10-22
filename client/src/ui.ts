/** Small DOM and formatting helpers shared by the views. Nothing here ever sets innerHTML from data. */

export function $<T extends HTMLElement>(selector: string): T {
  const el = document.querySelector<T>(selector);
  if (!el) {
    throw new Error(`Missing element: ${selector}`);
  }
  return el;
}

type Child = Node | string | null | undefined | false;

/** Builds an element. Strings become text nodes, so untrusted data can never inject markup. */
export function h<K extends keyof HTMLElementTagNameMap>(
  tag: K,
  props: Record<string, string | number | boolean | undefined> = {},
  ...children: Child[]
): HTMLElementTagNameMap[K] {
  const el = document.createElement(tag);
  for (const [key, value] of Object.entries(props)) {
    if (value === undefined || value === false) {
      continue;
    }
    if (key === "class") {
      el.className = String(value);
    } else if (value === true) {
      el.setAttribute(key, "");
    } else {
      el.setAttribute(key, String(value));
    }
  }
  for (const child of children) {
    if (child === null || child === undefined || child === false) {
      continue;
    }
    el.append(typeof child === "string" ? document.createTextNode(child) : child);
  }
  return el;
}

export function replaceChildren(parent: HTMLElement, ...children: Child[]): void {
  parent.replaceChildren(
    ...children
      .filter((c): c is Node | string => c !== null && c !== undefined && c !== false)
      .map((c) => (typeof c === "string" ? document.createTextNode(c) : c)),
  );
}

// Terminal control sequences (CSI, OSC, DCS/APC/PM/SOS strings, two-character escapes) and stray control characters.
const ESCAPE_SEQUENCES =
  // eslint-disable-next-line no-control-regex
  /\u001b(?:\[[0-?]*[ -/]*[@-~]|\][^\u0007\u001b]*(?:\u0007|\u001b\\)?|[PX^_][^\u001b]*(?:\u001b\\)?|[@-Z\\-_])/g;
// eslint-disable-next-line no-control-regex
const CONTROL_CHARS = /[\u0000-\u0008\u000b-\u001f\u007f-\u009f]/g;

/** Job output is untrusted text. It is only ever shown as text, but control sequences are still removed. */
export function clean(text: string): string {
  return text.replace(ESCAPE_SEQUENCES, "").replace(CONTROL_CHARS, "");
}

export function formatBytes(bytes: number): string {
  if (!Number.isFinite(bytes) || bytes <= 0) {
    return "0 B";
  }
  const units = ["B", "KB", "MB", "GB", "TB"];
  const i = Math.min(Math.floor(Math.log(bytes) / Math.log(1024)), units.length - 1);
  const v = bytes / 1024 ** i;
  return `${v.toFixed(v >= 100 ? 0 : v >= 10 ? 1 : 2)} ${units[i]}`;
}

export function formatAge(unixSeconds: number): string {
  if (!unixSeconds) {
    return "–";
  }
  const s = Math.max(0, Math.round(Date.now() / 1000 - unixSeconds));
  if (s < 60) return `${s}s`;
  if (s < 3600) return `${Math.floor(s / 60)}m`;
  if (s < 86400) return `${Math.floor(s / 3600)}h`;
  return `${Math.floor(s / 86400)}d`;
}

export function formatTime(unixSeconds: number): string {
  if (!unixSeconds) {
    return "–";
  }
  return new Intl.DateTimeFormat(undefined, { dateStyle: "medium", timeStyle: "short" }).format(new Date(unixSeconds * 1000));
}

export function clockTime(): string {
  return new Date().toLocaleTimeString(undefined, { hour12: false });
}

export function pill(text: string, extra = ""): HTMLSpanElement {
  return h("span", { class: `pill pill-${text.replace(/[^a-z]/gi, "").toLowerCase()} ${extra}`.trim() }, text);
}

/** A labelled usage bar with the 90% safety limit marked. */
export function meter(label: string, percent: number, limit = 90): HTMLElement {
  const v = Math.max(0, Math.min(100, Number.isFinite(percent) ? percent : 0));
  const tone = v >= limit ? "is-bad" : v >= limit - 15 ? "is-warn" : "";
  const fill = h("span", { class: `meter-fill ${tone}`.trim() });
  fill.style.setProperty("--v", v.toFixed(1));
  const track = h("span", { class: "meter-track", role: "meter", "aria-label": label, "aria-valuemin": 0, "aria-valuemax": 100, "aria-valuenow": Math.round(v) }, fill, h("span", { class: "meter-limit", title: `${limit}% safety limit` }));
  return h("div", { class: "meter" }, h("span", {}, label), track, h("b", {}, `${Math.round(v)}%`));
}

export function fact(label: string, value: string | Node): HTMLElement {
  return h("div", {}, h("dt", {}, label), h("dd", {}, value));
}

export function clamp(n: number, lo: number, hi: number): number {
  return Math.min(hi, Math.max(lo, n));
}
