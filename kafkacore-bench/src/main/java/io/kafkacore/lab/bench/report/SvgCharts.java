package io.kafkacore.lab.bench.report;

import java.util.List;
import java.util.Locale;

/** Minimal dependency-free SVG chart rendering for self-contained HTML reports. */
public final class SvgCharts {

    public static final String[] PALETTE = {"#2563eb", "#0d9488", "#d97706", "#db2777", "#7c3aed", "#64748b"};

    private static final int W = 760;
    private static final int H = 300;
    private static final int L = 64;
    private static final int R = 20;
    private static final int T = 36;
    private static final int B = 64;

    private SvgCharts() {
    }

    /** A named line series; xs and ys must have equal length. */
    public record Series(String name, double[] xs, double[] ys, String color) {
    }

    /** A vertical annotation (e.g. "fault injected") at an x value. */
    public record Marker(double x, String label, String color) {
    }

    /** Grouped vertical bars: one group per category, one bar per series. */
    public static String groupedBars(String title, List<String> categories, List<String> seriesNames,
                                     double[][] values, String yLabel) {
        double max = 0;
        for (double[] row : values) for (double v : row) max = Math.max(max, v);
        double[] ticks = niceTicks(max);
        double top = ticks[ticks.length - 1];
        StringBuilder s = open(title);
        axes(s, ticks, top, yLabel);
        int plotW = W - L - R;
        int plotH = H - T - B;
        int groups = categories.size();
        double groupW = (double) plotW / Math.max(1, groups);
        double barW = Math.min(28, groupW * 0.8 / Math.max(1, seriesNames.size()));
        for (int g = 0; g < groups; g++) {
            double gx = L + g * groupW + (groupW - barW * seriesNames.size()) / 2;
            for (int k = 0; k < seriesNames.size(); k++) {
                double v = values[g][k];
                double h = top == 0 ? 0 : v / top * plotH;
                double x = gx + k * barW;
                double y = T + plotH - h;
                s.append(String.format(Locale.ROOT,
                        "<rect x='%.1f' y='%.1f' width='%.1f' height='%.1f' rx='2' fill='%s'><title>%s · %s: %s</title></rect>",
                        x, y, barW - 2, h, PALETTE[k % PALETTE.length], esc(categories.get(g)),
                        esc(seriesNames.get(k)), fmt(v)));
            }
            s.append(String.format(Locale.ROOT,
                    "<text x='%.1f' y='%d' class='xl' text-anchor='middle'>%s</text>",
                    L + g * groupW + groupW / 2, T + plotH + 18, esc(shorten(categories.get(g), (int) (groupW / 6.5)))));
        }
        legend(s, seriesNames);
        return s.append("</svg>").toString();
    }

    /** Multi-series line chart sharing one y axis. */
    public static String lineChart(String title, List<Series> series, String xLabel, String yLabel) {
        return lineChart(title, series, xLabel, yLabel, List.of());
    }

    /** Line chart with vertical event markers drawn behind the series. */
    public static String lineChart(String title, List<Series> series, String xLabel, String yLabel, List<Marker> markers) {
        double maxY = 0, minX = Double.MAX_VALUE, maxX = -Double.MAX_VALUE;
        for (Series se : series) {
            for (int i = 0; i < se.xs().length; i++) {
                maxY = Math.max(maxY, se.ys()[i]);
                minX = Math.min(minX, se.xs()[i]);
                maxX = Math.max(maxX, se.xs()[i]);
            }
        }
        if (minX == Double.MAX_VALUE) { minX = 0; maxX = 1; }
        if (maxX == minX) maxX = minX + 1;
        double[] ticks = niceTicks(maxY);
        double top = ticks[ticks.length - 1];
        StringBuilder s = open(title);
        axes(s, ticks, top, yLabel);
        int plotW = W - L - R;
        int plotH = H - T - B;
        for (int i = 0; i <= 5; i++) {
            double xv = minX + (maxX - minX) * i / 5.0;
            double x = L + plotW * i / 5.0;
            s.append(String.format(Locale.ROOT, "<text x='%.1f' y='%d' class='xl' text-anchor='middle'>%s</text>",
                    x, T + plotH + 18, fmt(xv)));
        }
        s.append(String.format(Locale.ROOT, "<text x='%d' y='%d' class='al' text-anchor='middle'>%s</text>",
                L + plotW / 2, H - 26, esc(xLabel)));
        int row = 0;
        for (Marker m : markers) {
            if (m.x() < minX || m.x() > maxX) continue;
            double x = L + (m.x() - minX) / (maxX - minX) * plotW;
            s.append(String.format(Locale.ROOT,
                    "<line x1='%.1f' x2='%.1f' y1='%d' y2='%d' stroke='%s' stroke-width='1.5' stroke-dasharray='4 3'/>"
                            + "<text x='%.1f' y='%d' class='mk' fill='%s' style='fill:%s;font-weight:600'>%s</text>",
                    x, x, T, T + plotH, m.color(), x + 4, T + 12 + (row++ % 3) * 13, m.color(), m.color(), esc(m.label())));
        }
        for (Series se : series) {
            StringBuilder pts = new StringBuilder();
            for (int i = 0; i < se.xs().length; i++) {
                double x = L + (se.xs()[i] - minX) / (maxX - minX) * plotW;
                double y = T + plotH - (top == 0 ? 0 : se.ys()[i] / top * plotH);
                pts.append(String.format(Locale.ROOT, "%.1f,%.1f ", x, y));
            }
            s.append(String.format(Locale.ROOT,
                    "<polyline points='%s' fill='none' stroke='%s' stroke-width='2' stroke-linejoin='round'/>",
                    pts.toString().trim(), se.color()));
        }
        legend(s, series.stream().map(Series::name).toList(), series.stream().map(Series::color).toList());
        return s.append("</svg>").toString();
    }

    static double[] niceTicks(double max) {
        if (max <= 0) return new double[]{0, 1};
        double raw = max / 5;
        double mag = Math.pow(10, Math.floor(Math.log10(raw)));
        double norm = raw / mag;
        double step = (norm <= 1 ? 1 : norm <= 2 ? 2 : norm <= 2.5 ? 2.5 : norm <= 5 ? 5 : 10) * mag;
        int n = (int) Math.ceil(max / step);
        double[] t = new double[n + 1];
        for (int i = 0; i <= n; i++) t[i] = i * step;
        return t;
    }

    private static StringBuilder open(String title) {
        StringBuilder s = new StringBuilder();
        s.append(String.format(Locale.ROOT,
                "<svg viewBox='0 0 %d %d' role='img' aria-label='%s' class='chart' xmlns='http://www.w3.org/2000/svg'>",
                W, H, esc(title)));
        s.append(String.format(Locale.ROOT, "<text x='%d' y='22' class='ct'>%s</text>", L, esc(title)));
        return s;
    }

    private static void axes(StringBuilder s, double[] ticks, double top, String yLabel) {
        int plotH = H - T - B;
        for (double t : ticks) {
            double y = T + plotH - (top == 0 ? 0 : t / top * plotH);
            s.append(String.format(Locale.ROOT,
                    "<line x1='%d' x2='%d' y1='%.1f' y2='%.1f' class='grid'/><text x='%d' y='%.1f' class='yl' text-anchor='end'>%s</text>",
                    L, W - R, y, y, L - 8, y + 4, fmt(t)));
        }
        s.append(String.format(Locale.ROOT,
                "<text transform='translate(16,%d) rotate(-90)' class='al' text-anchor='middle'>%s</text>",
                T + plotH / 2, esc(yLabel)));
    }

    private static void legend(StringBuilder s, List<String> names) {
        legend(s, names, null);
    }

    private static void legend(StringBuilder s, List<String> names, List<String> colors) {
        int x = L;
        int y = H - 10;
        for (int i = 0; i < names.size(); i++) {
            String c = colors == null ? PALETTE[i % PALETTE.length] : colors.get(i);
            s.append(String.format(Locale.ROOT,
                    "<rect x='%d' y='%d' width='10' height='10' rx='2' fill='%s'/><text x='%d' y='%d' class='lg'>%s</text>",
                    x, y - 9, c, x + 14, y, esc(names.get(i))));
            x += 24 + names.get(i).length() * 7;
        }
    }

    public static String fmt(double v) {
        if (v == Math.rint(v) && Math.abs(v) < 1e15) {
            return String.format(Locale.ROOT, "%,d", (long) v);
        }
        return Math.abs(v) >= 100 ? String.format(Locale.ROOT, "%,.0f", v) : String.format(Locale.ROOT, "%.2f", v);
    }

    public static String esc(String s) {
        return s.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;").replace("'", "&#39;").replace("\"", "&quot;");
    }

    private static String shorten(String s, int max) {
        int m = Math.max(6, max);
        return s.length() <= m ? s : s.substring(0, m - 1) + "…";
    }
}
