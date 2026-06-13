import { Routes, Route, Navigate } from "react-router-dom";
import { RequireRole } from "@/components/RequireRole";
import { AdminLayout } from "@/layouts/AdminLayout";
import { UserLayout } from "@/layouts/UserLayout";
import { AdminDashboard } from "@/pages/admin/Dashboard";
import { Workspaces } from "@/pages/user/Workspaces";
import { Unauthorized } from "@/pages/Unauthorized";

export function AppRouter() {
  return (
    <Routes>
      <Route path="/" element={<Navigate to="/workspace" replace />} />

      <Route
        path="/admin"
        element={
          <RequireRole role="pipeline-admin">
            <AdminLayout />
          </RequireRole>
        }
      >
        <Route index element={<AdminDashboard />} />
        <Route path="*" element={<AdminDashboard />} />
      </Route>

      <Route
        path="/workspace"
        element={
          <RequireRole role="pipeline-user">
            <UserLayout />
          </RequireRole>
        }
      >
        <Route index element={<Workspaces />} />
        <Route path="*" element={<Workspaces />} />
      </Route>

      <Route path="/unauthorized" element={<Unauthorized />} />
    </Routes>
  );
}
