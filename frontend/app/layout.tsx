import type { Metadata } from "next";
import "./globals.css";
import { Shell } from "@/components/layout/Shell";

export const metadata: Metadata = {
  title: "KimiBot — Prediction Market Trader",
  description: "Real-time trading dashboard for Limitless prediction markets",
};

export default function RootLayout({ children }: { children: React.ReactNode }) {
  return (
    <html lang="en" className="dark">
      <body>
        <Shell>{children}</Shell>
      </body>
    </html>
  );
}
