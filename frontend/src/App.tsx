import { useState, useEffect, useReducer, FormEvent, createContext, useContext, ReactNode } from 'react'
import Dashboard from './Dashboard'
import './App.css'

const STORAGE_KEY = 'api_key'

interface Item {
  id: number
  type: string
  title: string
  created_at: string
}

type FetchState =
  | { status: 'idle' }
  | { status: 'loading' }
  | { status: 'success'; items: Item[] }
  | { status: 'error'; message: string }

type FetchAction =
  | { type: 'fetch_start' }
  | { type: 'fetch_success'; data: Item[] }
  | { type: 'fetch_error'; message: string }

function fetchReducer(_state: FetchState, action: FetchAction): FetchState {
  switch (action.type) {
    case 'fetch_start':
      return { status: 'loading' }
    case 'fetch_success':
      return { status: 'success', items: action.data }
    case 'fetch_error':
      return { status: 'error', message: action.message }
  }
}

type Page = 'items' | 'dashboard'

interface AuthContextType {
  token: string
  setToken: (token: string) => void
  handleDisconnect: () => void
  currentPage: Page
  setCurrentPage: (page: Page) => void
}

const AuthContext = createContext<AuthContextType | null>(null)

function useAuth() {
  const context = useContext(AuthContext)
  if (!context) {
    throw new Error('useAuth must be used within AuthProvider')
  }
  return context
}

function AuthProvider({ children }: { children: ReactNode }) {
  const [token, setTokenState] = useState(() => localStorage.getItem(STORAGE_KEY) ?? '')
  const [draft, setDraft] = useState('')
  const [currentPage, setCurrentPage] = useState<Page>('items')

  function setToken(token: string) {
    setTokenState(token)
  }

  function handleDisconnect() {
    localStorage.removeItem(STORAGE_KEY)
    setTokenState('')
    setDraft('')
    setCurrentPage('items')
  }

  function handleConnect(e: FormEvent) {
    e.preventDefault()
    const trimmed = draft.trim()
    if (!trimmed) return
    localStorage.setItem(STORAGE_KEY, trimmed)
    setToken(trimmed)
  }

  if (!token) {
    return (
      <form className="token-form" onSubmit={handleConnect}>
        <h1>API Key</h1>
        <p>Enter your API key to connect.</p>
        <input
          type="password"
          placeholder="Token"
          value={draft}
          onChange={(e) => setDraft(e.target.value)}
        />
        <button type="submit">Connect</button>
      </form>
    )
  }

  return (
    <AuthContext.Provider value={{ token, setToken, handleDisconnect, currentPage, setCurrentPage }}>
      {children}
    </AuthContext.Provider>
  )
}

function AppHeader() {
  const { handleDisconnect, currentPage, setCurrentPage } = useAuth()

  return (
    <header className="app-header">
      <nav className="nav-links">
        <button
          className={currentPage === 'items' ? 'active' : ''}
          onClick={() => setCurrentPage('items')}
        >
          Items
        </button>
        <button
          className={currentPage === 'dashboard' ? 'active' : ''}
          onClick={() => setCurrentPage('dashboard')}
        >
          Dashboard
        </button>
      </nav>
      <button className="btn-disconnect" onClick={handleDisconnect}>
        Disconnect
      </button>
    </header>
  )
}

function ItemsPage({ token }: { token: string }) {
  const [fetchState, dispatch] = useReducer(fetchReducer, { status: 'idle' })

  useEffect(() => {
    if (!token) return

    dispatch({ type: 'fetch_start' })

    fetch('/items/', {
      headers: { Authorization: `Bearer ${token}` },
    })
      .then((res) => {
        if (!res.ok) throw new Error(`HTTP ${res.status}`)
        return res.json()
      })
      .then((data: Item[]) => dispatch({ type: 'fetch_success', data }))
      .catch((err: Error) =>
        dispatch({ type: 'fetch_error', message: err.message }),
      )
  }, [token])

  return (
    <div>
      <h1>Items</h1>

      {fetchState.status === 'loading' && <p>Loading...</p>}
      {fetchState.status === 'error' && <p>Error: {fetchState.message}</p>}

      {fetchState.status === 'success' && (
        <table>
          <thead>
            <tr>
              <th>ID</th>
              <th>ItemType</th>
              <th>Title</th>
              <th>Created at</th>
            </tr>
          </thead>
          <tbody>
            {fetchState.items.map((item) => (
              <tr key={item.id}>
                <td>{item.id}</td>
                <td>{item.type}</td>
                <td>{item.title}</td>
                <td>{item.created_at}</td>
              </tr>
            ))}
          </tbody>
        </table>
      )}
    </div>
  )
}

function DashboardPage() {
  const { token } = useAuth()
  return <Dashboard token={token} />
}

function AppContent() {
  const { token, currentPage } = useAuth()

  if (!token) {
    return null
  }

  if (currentPage === 'items') {
    return <ItemsPage token={token} />
  }

  return <DashboardPage />
}

function App() {
  return (
    <AuthProvider>
      <AppHeader />
      <AppContent />
    </AuthProvider>
  )
}

export default App
