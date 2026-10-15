import data from './data/insights.json'   // exported from the data lake by export_site_data.py
import { ColumnChart, GroupedBarChart, HBarChart, Heatmap, ScatterChart, StackedBars } from './components/Charts.jsx'
import './App.css'

const pct = (v) => `${(v * 100).toFixed(0)}%`
const STATUS_COLORS = ['var(--status-high)', 'var(--status-medium)', 'var(--neutral)']

const QUESTIONS = [
  ['q1', 'What are the performance ratings and review history for each employee?', 'gold_employee_performance_history'],
  ['q2', 'Which employees have critical skill gaps in key competencies?', 'gold_skill_gaps'],
  ['q3', 'Who are the high-potential employees ready for promotion?', 'gold_promotion_pipeline'],
  ['q4', 'What training and development activities has each employee completed?', 'gold_employee_learning_summary'],
  ['q5', 'Which employees are at risk of attrition based on performance trends?', 'gold_attrition_risk'],
  ['q6', 'How does employee performance correlate with team and project outcomes?', 'gold_team_performance_outcomes'],
  ['q7', 'What is the distribution of skills across the organisation?', 'gold_skill_distribution'],
]

export default function App() {
  const { run, quality, kpis, questions: q } = data
  return (
    <>
      <Header run={run} data={data} />
      <nav className="toc" aria-label="Sections">
        <a href="#overview">Overview</a>
        <a href="#pipeline">Pipeline run</a>
        <a href="#quality">Data quality</a>
        {QUESTIONS.map(([id], i) => <a key={id} href={`#${id}`}>Q{i + 1}</a>)}
      </nav>
      <main className="page">
        <section id="overview">
          <div className="tiles">
            {kpis.map((k) => (
              <div key={k.label} className="tile">
                <div className="tile-label">{k.label}</div>
                <div className="tile-value">{k.value}</div>
                <div className="tile-note">{k.note}</div>
              </div>
            ))}
          </div>
          <Architecture run={run} />
        </section>

        <PipelineRun run={run} />
        <DataQuality quality={quality} />

        <Question id="q1" index={0} finding={q.q1.finding}>
          <GroupedBarChart label="Employees per rating by review period" categories={q.q1.ratings} series={q.q1.series}
            colors={['var(--series-1)', 'var(--series-2)']} xTitle="Rating" yTitle="Employees" />
          <Table columns={['Trend into latest review', 'Employees']} numeric={[1]} rows={Object.entries(q.q1.trend)} />
        </Question>

        <Question id="q2" index={1} finding={q.q2.finding} rule={`Critical gap = required level − proficiency ≥ ${q.q2.rule}`}>
          <h4>Competencies with the most critical gaps</h4>
          <HBarChart label="Critical gaps per competency"
            data={q.q2.top.map((t) => ({ label: t.name, value: t.critical, display: `${t.critical} (${pct(t.rate)})`,
              tooltip: `${t.critical} employees · ${pct(t.rate)} of assessed · ${t.category}` }))} />
          <h4>Critical gap rate by department and skill category</h4>
          <Heatmap label="Critical gap rate" rows={q.q2.heatmap.rows} cols={q.q2.heatmap.cols} values={q.q2.heatmap.values} format={pct} />
          <Table columns={['Employee', 'Department', 'Critical gaps', 'Competencies']} numeric={[2]}
            rows={q.q2.people.map((p) => [p.full_name, p.department, p.gaps, p.competencies])} />
        </Question>

        <Question id="q3" index={2} finding={q.q3.finding}
          rule={`Candidate = High potential + Ready Now + latest rating ≥ ${q.q3.rule}`}>
          <h4>Promotion candidates by department</h4>
          <StackedCounts rows={q.q3.by_department} />
          <Table columns={['Employee', 'Department', 'Level', 'Latest rating', 'Critical gaps', 'Stage']} numeric={[3, 4]}
            rows={q.q3.shortlist.map((s) => [s.full_name, s.department, s.job_level, s.latest_rating, s.critical_gap_count, s.pipeline_stage])} />
        </Question>

        <Question id="q4" index={3} finding={q.q4.finding}>
          <ColumnChart label="Courses completed per employee" data={q.q4.completed} xTitle="Courses completed" yTitle="Employees" />
          <h4>Average courses completed by department</h4>
          <HBarChart label="Average courses completed by department" data={q.q4.by_department} format={(v) => v.toFixed(2)} />
        </Question>

        <Question id="q5" index={4} finding={q.q5.finding}
          rule={`Points: HR High +${q.q5.rules.points.hr_high} / Medium +${q.q5.rules.points.hr_medium}, rating declined +${q.q5.rules.points.rating_declined}, low rating +${q.q5.rules.points.low_rating}, no recent learning +${q.q5.rules.points.learning_inactive} · High ≥ ${q.q5.rules.high}, Medium ≥ ${q.q5.rules.medium}`}>
          <div className="tiles small">
            {Object.entries(q.q5.tiers).map(([tier, n], i) => (
              <div key={tier} className="tile">
                <div className="tile-label"><span className="swatch" style={{ background: STATUS_COLORS[i] }} />{tier} risk</div>
                <div className="tile-value">{n.toLocaleString()}</div>
              </div>
            ))}
          </div>
          <h4>Risk tiers by department</h4>
          <StackedBars label="Risk tier share by department" keys={['High', 'Medium', 'Low']} colors={STATUS_COLORS}
            rows={q.q5.by_department.map((d) => ({ label: d.department, High: d.High, Medium: d.Medium, Low: d.Low }))} />
          <h4>What drives the scores</h4>
          <HBarChart label="Employees per risk factor" data={q.q5.factors} />
          <Table columns={['Employee', 'Department', 'Potential', 'Score', 'Reasons']} numeric={[3]}
            rows={q.q5.watchlist.map((w) => [w.full_name, w.department, w.potential_rating, w.risk_score, w.reasons])} />
        </Question>

        <Question id="q6" index={5} finding={q.q6.finding}>
          <ScatterChart label="Team rating vs project outcome" points={q.q6.points} slope={q.q6.slope} intercept={q.q6.intercept}
            xTitle="Team average latest rating" yTitle="Average project outcome score" />
          <p className="caption">r = {q.q6.r} · {q.q6.points.length} of {q.q6.teams_total} teams have projects · line = least-squares fit</p>
        </Question>

        <Question id="q7" index={6} finding={q.q7.finding}>
          <h4>Proficiency profile by skill category</h4>
          <Heatmap label="Share of employees per proficiency level" rows={q.q7.heatmap.rows} cols={q.q7.heatmap.cols}
            values={q.q7.heatmap.values} format={pct} />
          <div className="two-col">
            <div><h4>Weakest competencies</h4><HBarChart label="Lowest average proficiency" data={q.q7.weakest} format={(v) => v.toFixed(2)} labelWidth={230} /></div>
            <div><h4>Strongest competencies</h4><HBarChart label="Highest average proficiency" data={q.q7.strongest} format={(v) => v.toFixed(2)} labelWidth={230} /></div>
          </div>
        </Question>
      </main>
      <footer className="footer">
        <p>
          Built with PySpark, Pydantic, PostgreSQL (AWS Aurora), S3, Kubernetes (EKS), Terraform and React.
          Data: synthetic workshop sample. Figures exported from <code>{data.source}</code> on {new Date(data.generated_at).toLocaleString()}.
        </p>
        <p><a href={data.github}>Source code on GitHub</a></p>
      </footer>
    </>
  )
}

function Header({ run, data }) {
  const ok = run.status === 'SUCCEEDED'
  return (
    <header className="hero">
      <div className="hero-inner">
        <p className="eyebrow">ACME Inc. · Talent data platform</p>
        <h1>Talent Insights</h1>
        <p className="lede">
          A batch data pipeline (Bronze → Silver → Gold) on AWS that turns HR records and learning events into answers
          about performance, skills, promotion and attrition. Everything on this page comes from the latest cloud run.
        </p>
        <div className="run-badges">
          <span className={`status ${ok ? 'ok' : 'bad'}`}>{ok ? '✓' : '✕'} Last run {run.status.toLowerCase()}</span>
          <span className="meta">{run.environment}</span>
          <span className="meta">Run {run.run_id}</span>
          <span className="meta">{Math.round(run.duration_seconds)} s</span>
          <a className="meta link" href={data.github}>GitHub ↗</a>
        </div>
      </div>
    </header>
  )
}

function Architecture({ run }) {
  const stages = [
    ['Sources', 'Aurora PostgreSQL (7 HR tables) · S3 landing (783 daily JSONL files)'],
    ['Bronze', 'Raw copy + audit columns, partitioned by ingest date'],
    ['Silver', 'Pydantic contracts, dedup, foreign keys · bad rows → quarantine'],
    ['Gold', '7 business tables, grain + reconciliation checks'],
    ['Serving', 'Gold → S3 Parquet + Aurora gold_* → this site & Jupyter'],
  ]
  return (
    <div className="arch" aria-label="Architecture">
      {stages.map(([title, text], i) => (
        <div key={title} className="arch-step">
          <div className={`arch-box ${title.toLowerCase()}`}><strong>{title}</strong><span>{text}</span></div>
          {i < stages.length - 1 && <div className="arch-arrow" aria-hidden="true">→</div>}
        </div>
      ))}
      <p className="caption">Runs as a Kubernetes Job on EKS (job.py) · lineage manifest per run in <code>{run.lake}/lineage/</code></p>
    </div>
  )
}

function PipelineRun({ run }) {
  return (
    <section id="pipeline" className="section">
      <h2>Pipeline run</h2>
      <p className="section-lede">
        Run <code>{run.run_id}</code> for {run.run_date}, {run.status.toLowerCase()} in {Math.round(run.duration_seconds)} seconds.
        Every dataset reconciles: rows in = rows out + quarantined + duplicates removed.
      </p>
      <HBarChart label="Seconds per step" data={run.steps.map((s) => ({ label: `${s.name} (${s.status.toLowerCase()})`, value: s.seconds }))}
        format={(v) => `${v.toFixed(0)} s`} labelWidth={190} />
      <Table columns={['Layer', 'Dataset', 'Rows in', 'Rows out', 'Quarantined', 'Duplicates']} numeric={[2, 3, 4, 5]}
        rows={run.reconciliation.map((d) => [d.layer, d.dataset, d.rows_in, d.rows_out, d.rows_rejected, d.rows_dropped])} />
    </section>
  )
}

function DataQuality({ quality }) {
  return (
    <section id="quality" className="section">
      <h2>Data quality</h2>
      <p className="section-lede">
        The learning feed delivered {quality.lines_in.toLocaleString()} lines: {quality.kept.toLocaleString()} kept,{' '}
        {quality.quarantined.toLocaleString()} quarantined with a reason, {quality.duplicates.toLocaleString()} duplicates removed.{' '}
        {quality.hr_rejected === 0
          ? `The ${quality.hr_tables} HR tables passed every check with zero rejects.`
          : `The HR tables had ${quality.hr_rejected} rejected or duplicate rows.`}
      </p>
      <Table columns={['Issue found in the source data', 'Count', 'Handling', 'Detail']} numeric={[1]}
        rows={quality.issues.map((i) => [i.issue, i.count, i.handling, i.detail])} />
    </section>
  )
}

function Question({ id, index, finding, rule, children }) {
  const [, question, table] = QUESTIONS[index]
  return (
    <section id={id} className="section">
      <p className="eyebrow">Question {index + 1} · <code>{table}</code></p>
      <h2>{question}</h2>
      <div className="finding"><strong>Answer.</strong> {finding}</div>
      {rule && <p className="rule">Rule from the run manifest: {rule}</p>}
      {children}
    </section>
  )
}

function StackedCounts({ rows }) {
  const max = Math.max(...rows.map((r) => r.now + r.after), 1)
  return (
    <figure className="chart">
      <div className="stacked">
        {rows.map((r) => (
          <div key={r.department} className="stacked-row">
            <div className="stacked-label">{r.department}</div>
            <div className="stacked-bar counts">
              <div className="stacked-seg" style={{ width: `${(r.now / max) * 85}%`, background: 'var(--series-1)' }}
                title={`${r.department}: ${r.now} ready now`}>{r.now}</div>
              {r.after > 0 && (
                <div className="stacked-seg" style={{ width: `${(r.after / max) * 85}%`, background: 'var(--series-2)' }}
                  title={`${r.department}: ${r.after} after closing gaps`}>{r.after}</div>
              )}
              <span className="stacked-total">{r.now + r.after}</span>
            </div>
          </div>
        ))}
      </div>
      <div className="legend">
        <span><span className="swatch" style={{ background: 'var(--series-1)' }} />Promote now</span>
        <span><span className="swatch" style={{ background: 'var(--series-2)' }} />Promote after closing skill gaps</span>
      </div>
    </figure>
  )
}

function Table({ columns, rows, numeric = [] }) {
  return (
    <div className="table-wrap">
      <table>
        <thead><tr>{columns.map((c, i) => <th key={c} className={numeric.includes(i) ? 'num' : ''}>{c}</th>)}</tr></thead>
        <tbody>
          {rows.map((r, ri) => (
            <tr key={ri}>{r.map((v, ci) => (
              <td key={ci} className={numeric.includes(ci) ? 'num' : ''}>{typeof v === 'number' ? v.toLocaleString() : v}</td>
            ))}</tr>
          ))}
        </tbody>
      </table>
    </div>
  )
}
