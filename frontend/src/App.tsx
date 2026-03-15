import { Routes, Route, Link, useLocation } from 'react-router-dom'
import WaardebepalingPage from './pages/WaardebepalingPage'
import WoningenPage from './pages/WoningenPage'
import WatchlistPage from './pages/WatchlistPage'
import BuurtenPage from './pages/BuurtenPage'

function NavLink({ to, children }: { to: string; children: React.ReactNode }) {
  const location = useLocation()
  const isActive = location.pathname === to

  return (
    <Link
      to={to}
      className={`px-4 py-2 rounded-lg transition-colors ${
        isActive
          ? 'bg-primary-600 text-white'
          : 'text-gray-700 hover:bg-gray-100'
      }`}
    >
      {children}
    </Link>
  )
}

function App() {
  return (
    <div className="min-h-screen bg-gray-50">
      {/* Navigation */}
      <nav className="bg-white shadow-sm border-b">
        <div className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8">
          <div className="flex justify-between h-16">
            <div className="flex items-center">
              <Link to="/" className="text-xl font-bold text-primary-700">
                Woningzoeker
              </Link>
              <span className="ml-2 text-sm text-gray-500">
                Den Haag Regio
              </span>
            </div>
            <div className="flex items-center space-x-2">
              <NavLink to="/">Waardebepaling</NavLink>
              <NavLink to="/woningen">Woningen</NavLink>
              <NavLink to="/buurten">Buurten</NavLink>
              <NavLink to="/watchlist">Watchlist</NavLink>
            </div>
          </div>
        </div>
      </nav>

      {/* Main content */}
      <main className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8 py-8">
        <Routes>
          <Route path="/" element={<WaardebepalingPage />} />
          <Route path="/woningen" element={<WoningenPage />} />
          <Route path="/buurten" element={<BuurtenPage />} />
          <Route path="/watchlist" element={<WatchlistPage />} />
        </Routes>
      </main>
    </div>
  )
}

export default App
