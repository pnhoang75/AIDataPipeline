import { Outlet } from "react-router-dom";

export function UserLayout() {
  return (
    <div className="flex h-screen bg-background">
      <aside className="w-64 border-r border-border bg-card p-4">
        <h2 className="text-lg font-semibold mb-6">Workspace</h2>
        <nav className="space-y-2">
          <a href="/workspace" className="block px-3 py-2 rounded-md hover:bg-accent text-sm">Workspaces</a>
          <a href="/workspace/sources" className="block px-3 py-2 rounded-md hover:bg-accent text-sm">Data Sources</a>
          <a href="/workspace/files" className="block px-3 py-2 rounded-md hover:bg-accent text-sm">Files</a>
        </nav>
      </aside>
      <main className="flex-1 overflow-auto p-6">
        <Outlet />
      </main>
    </div>
  );
}
