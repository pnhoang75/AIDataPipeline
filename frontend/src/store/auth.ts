import { create } from "zustand";

interface AuthState {
  roles: string[];
  setRoles: (roles: string[]) => void;
  hasRole: (role: string) => boolean;
}

export const useAuthStore = create<AuthState>((set, get) => ({
  roles: [],
  setRoles: (roles) => set({ roles }),
  hasRole: (role) => get().roles.includes(role),
}));
