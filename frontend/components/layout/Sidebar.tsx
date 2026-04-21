"use client";

import Link from "next/link";
import { usePathname } from "next/navigation";
import { LayoutDashboard, Briefcase, BarChart2, Activity, ChevronRight } from "lucide-react";
import { cn } from "@/lib/utils";

const NAV = [
  { href: "/",          icon: LayoutDashboard, label: "Dashboard"  },
  { href: "/portfolio", icon: Briefcase,        label: "Portfolio"  },
  { href: "/analytics", icon: BarChart2,         label: "Analytics" },
  { href: "/health",    icon: Activity,          label: "System"    },
];

export function Sidebar() {
  const path = usePathname();

  return (
    <aside className="hidden lg:flex flex-col w-52 shrink-0 border-r border-border bg-bg-surface">
      {/* Logo */}
      <div className="px-5 py-4 border-b border-border">
        <div className="flex items-center gap-2.5">
          <div className="w-7 h-7 rounded bg-green/20 flex items-center justify-center">
            <span className="text-green font-mono font-bold text-sm">K</span>
          </div>
          <div>
            <div className="font-semibold text-md text-text-primary leading-none">KimiBot</div>
            <div className="text-2xs text-text-secondary mt-0.5">Prediction Trader</div>
          </div>
        </div>
      </div>

      {/* Nav */}
      <nav className="flex-1 px-2 py-3 space-y-0.5">
        {NAV.map(({ href, icon: Icon, label }) => {
          const active = href === "/" ? path === "/" : path.startsWith(href);
          return (
            <Link
              key={href}
              href={href}
              className={cn(
                "flex items-center gap-3 px-3 py-2 rounded-md text-sm transition-colors group",
                active
                  ? "bg-green/10 text-green"
                  : "text-text-secondary hover:text-text-primary hover:bg-bg-elevated"
              )}
            >
              <Icon size={15} className="shrink-0" />
              <span className="flex-1">{label}</span>
              {active && <ChevronRight size={12} className="text-green/50" />}
            </Link>
          );
        })}
      </nav>

      {/* Footer */}
      <div className="px-4 py-3 border-t border-border">
        <div className="text-2xs text-text-muted">v0.1.0 · walk-forward</div>
      </div>
    </aside>
  );
}
