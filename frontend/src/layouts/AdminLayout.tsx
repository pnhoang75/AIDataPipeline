import { NavLink, Outlet } from "react-router-dom";
import { cn } from "@/lib/utils";

const navItems = [
  { to: "/admin", label: "Dashboard", end: true },
  { to: "/admin/connectors", label: "Connectors" },
  { to: "/admin/pipeline", label: "Pipeline" },
  { to: "/admin/tenants", label: "Tenants" },
  { to: "/admin/quota", label: "Quota" },
];

export function AdminLayout() {
  return (
    <div className="flex h-screen bg-background">
      <aside className="w-56 border-r border-border bg-card p-4 flex-shrink-0">
        <h2 className="text-lg font-semibold mb-6">Admin</h2>
        <nav className="space-y-1">
          {navItems.map(({ to, label, end }) => (
            <NavLink
              key={to}
              to={to}
              end={end}
              className={({ isActive }) =>
                cn(
                  "block px-3 py-2 rounded-md text-sm transition-colors",
                  isActive
                    ? "bg-primary text-primary-foreground"
                    : "hover:bg-accent text-foreground"
                )
              }
            >
              {label}
            </NavLink>
          ))}
        </nav>
      </aside>
      <main className="flex-1 overflow-auto p-6">
        <Outlet />
      </main>
    </div>
  );
}
