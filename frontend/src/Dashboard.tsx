import { useState, useEffect, ChangeEvent } from 'react'
import {
  Chart as ChartJS,
  CategoryScale,
  LinearScale,
  BarElement,
  LineElement,
  PointElement,
  Title,
  Tooltip,
  Legend,
} from 'chart.js'
import { Bar, Line } from 'react-chartjs-2'
import './Dashboard.css'

ChartJS.register(
  CategoryScale,
  LinearScale,
  BarElement,
  LineElement,
  PointElement,
  Title,
  Tooltip,
  Legend,
)

// API Response Types
interface ScoreBucket {
  bucket: string
  count: number
}

interface TimelineEntry {
  date: string
  submissions: number
}

interface TaskPassRate {
  task: string
  avg_score: number
  attempts: number
}

interface Lab {
  id: number
  title: string
}

// Fetch state types for each data source
type ScoresFetchState =
  | { status: 'idle' }
  | { status: 'loading' }
  | { status: 'success'; data: ScoreBucket[] }
  | { status: 'error'; message: string }

type TimelineFetchState =
  | { status: 'idle' }
  | { status: 'loading' }
  | { status: 'success'; data: TimelineEntry[] }
  | { status: 'error'; message: string }

type PassRatesFetchState =
  | { status: 'idle' }
  | { status: 'loading' }
  | { status: 'success'; data: TaskPassRate[] }
  | { status: 'error'; message: string }

type LabsFetchState =
  | { status: 'idle' }
  | { status: 'loading' }
  | { status: 'success'; data: Lab[] }
  | { status: 'error'; message: string }

// Chart data preparation helpers
function prepareScoreChartData(buckets: ScoreBucket[]): {
  labels: string[]
  datasets: {
    label: string
    data: number[]
    backgroundColor: string
  }[]
} {
  return {
    labels: buckets.map((b) => b.bucket),
    datasets: [
      {
        label: 'Number of Students',
        data: buckets.map((b) => b.count),
        backgroundColor: 'rgba(54, 162, 235, 0.6)',
      },
    ],
  }
}

function prepareTimelineData(entries: TimelineEntry[]): {
  labels: string[]
  datasets: {
    label: string
    data: number[]
    borderColor: string
    backgroundColor: string
    fill: boolean
  }[]
} {
  return {
    labels: entries.map((e) => e.date),
    datasets: [
      {
        label: 'Submissions per Day',
        data: entries.map((e) => e.submissions),
        borderColor: 'rgba(75, 192, 192, 1)',
        backgroundColor: 'rgba(75, 192, 192, 0.2)',
        fill: true,
      },
    ],
  }
}

interface DashboardProps {
  token: string
}

function Dashboard({ token }: DashboardProps) {
  const [selectedLab, setSelectedLab] = useState<string>('')

  const [scoresState, setScoresState] = useState<ScoresFetchState>({ status: 'idle' })
  const [timelineState, setTimelineState] = useState<TimelineFetchState>({ status: 'idle' })
  const [passRatesState, setPassRatesState] = useState<PassRatesFetchState>({ status: 'idle' })
  const [labsState, setLabsState] = useState<LabsFetchState>({ status: 'idle' })

  // Fetch available labs on mount
  useEffect(() => {
    if (!token) return

    async function fetchLabs() {
      setLabsState({ status: 'loading' })
      try {
        const res = await fetch('/items/', {
          headers: { Authorization: `Bearer ${token}` },
        })
        if (!res.ok) throw new Error(`HTTP ${res.status}`)
        const items = (await res.json()) as { id: number; type: string; title: string }[]
        const labs = items
          .filter((item) => item.type === 'lab')
          .map((item) => ({ id: item.id, title: item.title }))
        setLabsState({ status: 'success', data: labs })
      } catch (err) {
        setLabsState({ status: 'error', message: err instanceof Error ? err.message : 'Unknown error' })
      }
    }

    fetchLabs()
  }, [token])

  // Fetch analytics data when lab selection changes
  useEffect(() => {
    if (!token || !selectedLab) return

    const fetchScores = async () => {
      setScoresState({ status: 'loading' })
      try {
        const res = await fetch(`/analytics/scores?lab=${encodeURIComponent(selectedLab)}`, {
          headers: { Authorization: `Bearer ${token}` },
        })
        if (!res.ok) throw new Error(`HTTP ${res.status}`)
        const data = (await res.json()) as ScoreBucket[]
        setScoresState({ status: 'success', data })
      } catch (err) {
        setScoresState({ status: 'error', message: err instanceof Error ? err.message : 'Unknown error' })
      }
    }

    const fetchTimeline = async () => {
      setTimelineState({ status: 'loading' })
      try {
        const res = await fetch(`/analytics/timeline?lab=${encodeURIComponent(selectedLab)}`, {
          headers: { Authorization: `Bearer ${token}` },
        })
        if (!res.ok) throw new Error(`HTTP ${res.status}`)
        const data = (await res.json()) as TimelineEntry[]
        setTimelineState({ status: 'success', data })
      } catch (err) {
        setTimelineState({ status: 'error', message: err instanceof Error ? err.message : 'Unknown error' })
      }
    }

    const fetchPassRates = async () => {
      setPassRatesState({ status: 'loading' })
      try {
        const res = await fetch(`/analytics/pass-rates?lab=${encodeURIComponent(selectedLab)}`, {
          headers: { Authorization: `Bearer ${token}` },
        })
        if (!res.ok) throw new Error(`HTTP ${res.status}`)
        const data = (await res.json()) as TaskPassRate[]
        setPassRatesState({ status: 'success', data })
      } catch (err) {
        setPassRatesState({ status: 'error', message: err instanceof Error ? err.message : 'Unknown error' })
      }
    }

    fetchScores()
    fetchTimeline()
    fetchPassRates()
  }, [token, selectedLab])

  function handleLabChange(e: ChangeEvent<HTMLSelectElement>) {
    setSelectedLab(e.target.value)
  }

  // Chart options
  const chartOptions = {
    responsive: true,
    maintainAspectRatio: false,
  }

  return (
    <div className="dashboard">
      <header className="dashboard-header">
        <h1>Analytics Dashboard</h1>
      </header>

      <div className="lab-selector">
        <label htmlFor="lab-select">Select Lab:</label>
        <select
          id="lab-select"
          value={selectedLab}
          onChange={handleLabChange}
          disabled={labsState.status !== 'success'}
        >
          <option value="">-- Select a lab --</option>
          {labsState.status === 'success' &&
            labsState.data.map((lab) => (
              <option key={lab.id} value={lab.title}>
                {lab.title}
              </option>
            ))}
        </select>
        {labsState.status === 'loading' && <span className="loading">Loading labs...</span>}
        {labsState.status === 'error' && <span className="error">Error: {labsState.message}</span>}
      </div>

      {!selectedLab ? (
        <p className="no-selection">Please select a lab to view analytics.</p>
      ) : (
        <div className="charts-container">
          {/* Score Distribution Bar Chart */}
          <div className="chart-card">
            <h2>Score Distribution</h2>
            {scoresState.status === 'loading' && <p>Loading...</p>}
            {scoresState.status === 'error' && <p className="error">Error: {scoresState.message}</p>}
            {scoresState.status === 'success' && scoresState.data.length === 0 && (
              <p>No score data available.</p>
            )}
            {scoresState.status === 'success' && scoresState.data.length > 0 && (
              <div className="chart-wrapper">
                <Bar data={prepareScoreChartData(scoresState.data)} options={chartOptions} />
              </div>
            )}
          </div>

          {/* Timeline Line Chart */}
          <div className="chart-card">
            <h2>Submissions Timeline</h2>
            {timelineState.status === 'loading' && <p>Loading...</p>}
            {timelineState.status === 'error' && <p className="error">Error: {timelineState.message}</p>}
            {timelineState.status === 'success' && timelineState.data.length === 0 && (
              <p>No timeline data available.</p>
            )}
            {timelineState.status === 'success' && timelineState.data.length > 0 && (
              <div className="chart-wrapper">
                <Line data={prepareTimelineData(timelineState.data)} options={chartOptions} />
              </div>
            )}
          </div>

          {/* Pass Rates Table */}
          <div className="chart-card">
            <h2>Pass Rates per Task</h2>
            {passRatesState.status === 'loading' && <p>Loading...</p>}
            {passRatesState.status === 'error' && <p className="error">Error: {passRatesState.message}</p>}
            {passRatesState.status === 'success' && passRatesState.data.length === 0 && (
              <p>No pass rate data available.</p>
            )}
            {passRatesState.status === 'success' && passRatesState.data.length > 0 && (
              <table>
                <thead>
                  <tr>
                    <th>Task</th>
                    <th>Avg Score</th>
                    <th>Attempts</th>
                  </tr>
                </thead>
                <tbody>
                  {passRatesState.data.map((task, index) => (
                    <tr key={index}>
                      <td>{task.task}</td>
                      <td>{task.avg_score}</td>
                      <td>{task.attempts}</td>
                    </tr>
                  ))}
                </tbody>
              </table>
            )}
          </div>
        </div>
      )}
    </div>
  )
}

export default Dashboard
