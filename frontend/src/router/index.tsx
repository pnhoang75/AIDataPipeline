import { Routes, Route, Navigate } from "react-router-dom";
import { RequireRole } from "@/components/RequireRole";
import { AdminLayout } from "@/layouts/AdminLayout";
import { UserLayout } from "@/layouts/UserLayout";
import { AdminDashboard } from "@/pages/admin/Dashboard";
import { Connectors } from "@/pages/admin/Connectors";
import { PipelineTuning } from "@/pages/admin/Pipeline";
import { Tenants } from "@/pages/admin/Tenants";
import { QuotaManagement } from "@/pages/admin/Quota";
import { Workspaces } from "@/pages/user/Workspaces";
import { DataSources } from "@/pages/user/DataSources";
import { FileBrowser } from "@/pages/user/FileBrowser";
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
        <Route path="connectors" element={<Connectors />} />
        <Route path="pipeline" element={<PipelineTuning />} />
        <Route path="tenants" element={<Tenants />} />
        <Route path="quota" element={<QuotaManagement />} />
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
        <Route path="sources" element={<DataSources />} />
        <Route path="files" element={<FileBrowser />} />
        <Route path="*" element={<Workspaces />} />
      </Route>

      <Route path="/unauthorized" element={<Unauthorized />} />
    </Routes>
  );
}
