export const cx = (...values: Array<string | false | null | undefined>) =>
  values.filter(Boolean).join(" ");

export const titleCase = (value: string) =>
  value.replaceAll("_", " ").replace(/\b\w/g, (letter) => letter.toUpperCase());

export const formatDate = (value?: string | null) =>
  value
    ? new Intl.DateTimeFormat(undefined, { dateStyle: "medium", timeStyle: "short" }).format(new Date(value))
    : "—";

export const formatBytes = (value?: number | null) => {
  if (value == null) return "—";
  const units = ["B", "KB", "MB", "GB"];
  let size = value;
  let unit = 0;
  while (size >= 1024 && unit < units.length - 1) { size /= 1024; unit += 1; }
  return `${size.toFixed(unit ? 1 : 0)} ${units[unit]}`;
};

export const initials = (name?: string | null, email?: string) =>
  (name || email || "S").split(/[\s@]+/).slice(0, 2).map((part) => part[0]?.toUpperCase()).join("");
