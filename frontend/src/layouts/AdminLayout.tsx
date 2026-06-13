import { Outlet } from "react-router-dom";

export function AdminLayout() {
  return (
    <div className="flex h-screen bg-background">
      <aside className="w-64 border-r border-border bg-card p-4">
        <h2 className="text-lg font-semibold mb-6">Admin</h2>
        <nav className="space-y-2">
          <a href="/admin" className="block px-3 py-2 rounded-md hover:bg-accent text-sm">Dashboard</a>
          <a href="/admin/connectors" className="block px-3 py-2 rounded-md hover:bg-accent text-sm">Connectors</a>
          <a href="/admin/pipeline" className="block px-3 py-2 rounded-md hover:bg-accent text-sm">Pipeline</a>
          <a href="/admin/tenants" className="block px-3 py-2 rounded-md hover:bg-accent text-sm">Tenants</a>
          <a href="/admin/quota" className="block px-3 py-2 rounded-md hover:bg-accent text-sm">Quota</a>
        </nav>
      </aside>
      <main className="flex-1 overflow-auto p-6">
        <Outlet />
      </main>
    </div>
  );
}
