import { useState } from 'react'

/* Small, dependency-free SVG/HTML charts with hover tooltips.
   Colours come from CSS tokens (index.css) so light and dark mode both work. */

const BLUES = ['#cde2fb', '#9ec5f4', '#6da7ec', '#3987e5', '#256abf', '#184f95', '#0d366b']

/** Sequential one-hue colour for t in [0, 1]. */
function sequential(t) {
  const clamped = Math.max(0, Math.min(1, Number.isFinite(t) ? t : 0))
  return BLUES[Math.round(clamped * (BLUES.length - 1))]
}

/** Readable text colour on a sequential cell. */
function inkOn(t) {
  return t > 0.5 ? '#ffffff' : '#0b0b0b'
}

function useTooltip() {
  const [tip, setTip] = useState(null)
  const bind = (content) => ({
    onMouseMove: (e) => {
      const box = e.currentTarget.closest('.chart').getBoundingClientRect()
      setTip({ x: e.clientX - box.left + 14, y: e.clientY - box.top + 14, content })
    },
    onMouseLeave: () => setTip(null),
  })
  return [tip, bind]
}

function Frame({ tip, children, label }) {
  return (
    <figure className="chart" aria-label={label}>
      {children}
      {tip && (
        <div className="tooltip" style={{ left: tip.x, top: tip.y }}>
          {tip.content}
        </div>
      )}
    </figure>
  )
}

/** Horizontal bars: one series (or per-item colour), labels on the left, value at the bar end. */
export function HBarChart({ data, format = (v) => v.toLocaleString(), label, labelWidth = 210 }) {
  const [tip, bind] = useTooltip()
  const width = 680
  const row = 30
  const valueSpace = 90
  const max = Math.max(...data.map((d) => d.value), 1)
  const height = data.length * row + 6
  return (
    <Frame tip={tip} label={label}>
      <svg viewBox={`0 0 ${width} ${height}`} width="100%" role="img" aria-label={label}>
        {data.map((d, i) => {
          const barWidth = Math.max(2, ((width - labelWidth - valueSpace) * d.value) / max)
          const y = i * row + 3
          return (
            <g key={d.label} {...bind(<><strong>{d.label}</strong><br />{d.tooltip ?? format(d.value)}</>)}>
              <rect x="0" y={y} width={width} height={row} fill="transparent" />
              <text x={labelWidth - 10} y={y + row / 2} className="axis-label" textAnchor="end" dominantBaseline="middle">
                {d.label}
              </text>
              <rect x={labelWidth} y={y + 7} width={barWidth} height={row - 14} rx="3" fill={d.color ?? 'var(--series-1)'} />
              <text x={labelWidth + barWidth + 8} y={y + row / 2} className="value-label" dominantBaseline="middle">
                {d.display ?? format(d.value)}
              </text>
            </g>
          )
        })}
      </svg>
    </Frame>
  )
}

/** Vertical grouped bars: categories on x, one bar per series. */
export function GroupedBarChart({ categories, series, colors, label, xTitle, yTitle }) {
  const [tip, bind] = useTooltip()
  const width = 680
  const height = 300
  const pad = { left: 52, right: 12, top: 16, bottom: 44 }
  const max = Math.max(...series.flatMap((s) => s.values), 1)
  const plotW = width - pad.left - pad.right
  const plotH = height - pad.top - pad.bottom
  const group = plotW / categories.length
  const barW = (group * 0.72) / series.length
  const ticks = niceTicks(max)
  const y = (v) => pad.top + plotH - (v / ticks.at(-1)) * plotH
  return (
    <Frame tip={tip} label={label}>
      <svg viewBox={`0 0 ${width} ${height}`} width="100%" role="img" aria-label={label}>
        {ticks.map((t) => (
          <g key={t}>
            <line x1={pad.left} x2={width - pad.right} y1={y(t)} y2={y(t)} className="grid" />
            <text x={pad.left - 8} y={y(t)} className="axis-label" textAnchor="end" dominantBaseline="middle">{t.toLocaleString()}</text>
          </g>
        ))}
        {categories.map((c, ci) => (
          <g key={c}>
            {series.map((s, si) => {
              const v = s.values[ci]
              const x = pad.left + ci * group + group * 0.14 + si * (barW + 2)
              return (
                <g key={s.name} {...bind(<><strong>{s.name}</strong> · {xTitle} {c}<br />{v.toLocaleString()} {yTitle?.toLowerCase()}</>)}>
                  <rect x={x} y={y(v)} width={barW} height={pad.top + plotH - y(v)} rx="3" fill={colors[si]} />
                </g>
              )
            })}
            <text x={pad.left + ci * group + group / 2} y={height - pad.bottom + 18} className="axis-label" textAnchor="middle">{c}</text>
          </g>
        ))}
        <text x={pad.left + plotW / 2} y={height - 6} className="axis-title" textAnchor="middle">{xTitle}</text>
      </svg>
      {series.length > 1 && <Legend items={series.map((s, i) => ({ label: s.name, color: colors[i] }))} />}
    </Frame>
  )
}

/** Vertical bars for one series (no legend: the title names it). */
export function ColumnChart({ data, label, xTitle, yTitle }) {
  return (
    <GroupedBarChart
      categories={data.map((d) => d.label)}
      series={[{ name: yTitle, values: data.map((d) => d.value) }]}
      colors={['var(--series-1)']}
      label={label}
      xTitle={xTitle}
      yTitle={yTitle}
    />
  )
}

/** Scatter plot with a least-squares line. */
export function ScatterChart({ points, slope, intercept, label, xTitle, yTitle }) {
  const [tip, bind] = useTooltip()
  const width = 680
  const height = 340
  const pad = { left: 56, right: 16, top: 16, bottom: 48 }
  const xs = points.map((p) => p.x)
  const ys = points.map((p) => p.y)
  const [x0, x1] = [Math.floor(Math.min(...xs) * 4) / 4, Math.ceil(Math.max(...xs) * 4) / 4]
  const [y0, y1] = [Math.floor(Math.min(...ys) / 10) * 10, Math.ceil(Math.max(...ys) / 10) * 10]
  const sx = (v) => pad.left + ((v - x0) / (x1 - x0)) * (width - pad.left - pad.right)
  const sy = (v) => pad.top + (1 - (v - y0) / (y1 - y0)) * (height - pad.top - pad.bottom)
  const xTicks = range(x0, x1, 0.25)
  const yTicks = range(y0, y1, 10)
  return (
    <Frame tip={tip} label={label}>
      <svg viewBox={`0 0 ${width} ${height}`} width="100%" role="img" aria-label={label}>
        {yTicks.map((t) => (
          <g key={`y${t}`}>
            <line x1={pad.left} x2={width - pad.right} y1={sy(t)} y2={sy(t)} className="grid" />
            <text x={pad.left - 8} y={sy(t)} className="axis-label" textAnchor="end" dominantBaseline="middle">{t}</text>
          </g>
        ))}
        {xTicks.map((t) => (
          <text key={`x${t}`} x={sx(t)} y={height - pad.bottom + 18} className="axis-label" textAnchor="middle">{t.toFixed(2)}</text>
        ))}
        <line x1={sx(x0)} y1={sy(slope * x0 + intercept)} x2={sx(x1)} y2={sy(slope * x1 + intercept)} className="trend" />
        {points.map((p) => (
          <circle key={p.team} cx={sx(p.x)} cy={sy(p.y)} r="7" className="point"
            {...bind(<><strong>{p.team}</strong><br />avg rating {p.x} · outcome {p.y}<br />{p.projects} project(s)</>)} />
        ))}
        <text x={pad.left + (width - pad.left - pad.right) / 2} y={height - 8} className="axis-title" textAnchor="middle">{xTitle}</text>
        <text x={14} y={pad.top + (height - pad.top - pad.bottom) / 2} className="axis-title" textAnchor="middle"
          transform={`rotate(-90 14 ${pad.top + (height - pad.top - pad.bottom) / 2})`}>{yTitle}</text>
      </svg>
    </Frame>
  )
}

/** Heatmap as an HTML grid (responsive), sequential blue scale. */
export function Heatmap({ rows, cols, values, format, label }) {
  const [tip, bind] = useTooltip()
  const flat = values.flat()
  const [min, max] = [Math.min(...flat), Math.max(...flat)]
  return (
    <Frame tip={tip} label={label}>
      <div className="heatmap" style={{ gridTemplateColumns: `minmax(110px, 1.4fr) repeat(${cols.length}, 1fr)` }}>
        <div />
        {cols.map((c) => <div key={c} className="heat-col">{c}</div>)}
        {rows.map((r, ri) => (
          <div key={r} className="heat-row">
            <div className="heat-label">{r}</div>
            {values[ri].map((v, ci) => {
              const t = max > min ? (v - min) / (max - min) : 0.5
              return (
                <div key={cols[ci]} className="heat-cell" style={{ background: sequential(t), color: inkOn(t) }}
                  {...bind(<><strong>{r}</strong> · {cols[ci]}<br />{format(v)}</>)}>
                  {format(v)}
                </div>
              )
            })}
          </div>
        ))}
      </div>
    </Frame>
  )
}

/** 100% stacked horizontal bars (e.g. risk tiers per department). */
export function StackedBars({ rows, keys, colors, label }) {
  const [tip, bind] = useTooltip()
  return (
    <Frame tip={tip} label={label}>
      <div className="stacked">
        {rows.map((r) => (
          <div key={r.label} className="stacked-row">
            <div className="stacked-label">{r.label}</div>
            <div className="stacked-bar">
              {keys.filter((k) => r[k] > 0).map((k) => (
                <div key={k} className="stacked-seg" style={{ width: `${r[k] * 100}%`, background: colors[keys.indexOf(k)] }}
                  {...bind(<><strong>{r.label}</strong><br />{k} risk: {(r[k] * 100).toFixed(0)}%</>)}>
                  {k === keys[0] && r[k] >= 0.06 ? `${(r[k] * 100).toFixed(0)}%` : ''}
                </div>
              ))}
            </div>
          </div>
        ))}
      </div>
      <Legend items={keys.map((k, i) => ({ label: `${k} risk`, color: colors[i] }))} />
    </Frame>
  )
}

export function Legend({ items }) {
  return (
    <div className="legend">
      {items.map((i) => (
        <span key={i.label}><span className="swatch" style={{ background: i.color }} />{i.label}</span>
      ))}
    </div>
  )
}

function niceTicks(max) {
  const step = 10 ** Math.floor(Math.log10(max)) * (max / 10 ** Math.floor(Math.log10(max)) > 5 ? 2 : 1)
  const top = Math.ceil(max / step) * step
  return range(0, top, step)
}

function range(start, end, step) {
  const out = []
  for (let v = start; v <= end + step / 1000; v += step) out.push(Math.round(v * 100) / 100)
  return out
}
