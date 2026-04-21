import type { Config } from "tailwindcss";

const config: Config = {
  content: [
    "./pages/**/*.{js,ts,jsx,tsx,mdx}",
    "./components/**/*.{js,ts,jsx,tsx,mdx}",
    "./app/**/*.{js,ts,jsx,tsx,mdx}",
  ],
  theme: {
    extend: {
      colors: {
        bg: {
          DEFAULT: "#080c14",
          surface: "#0c1220",
          card: "#111826",
          elevated: "#16202e",
        },
        border: {
          DEFAULT: "#1a2640",
          subtle: "#131e30",
        },
        text: {
          primary: "#e2e8f0",
          secondary: "#64748b",
          muted: "#2d3f55",
        },
        green: {
          DEFAULT: "#00d4aa",
          dim: "#0a8060",
          glow: "rgba(0,212,170,0.12)",
          "50": "#e6fff9",
          "400": "#00d4aa",
          "500": "#00b890",
        },
        red: {
          DEFAULT: "#f05e6e",
          dim: "#7f1d2e",
          glow: "rgba(240,94,110,0.12)",
          "400": "#f05e6e",
          "500": "#d9394b",
        },
        yellow: {
          DEFAULT: "#f0b429",
          dim: "#7a5c12",
          "400": "#f0b429",
        },
        blue: {
          DEFAULT: "#3b82f6",
          dim: "#1e3a6e",
          "400": "#3b82f6",
        },
      },
      fontFamily: {
        sans: ["Outfit", "system-ui", "sans-serif"],
        mono: ['"IBM Plex Mono"', "Menlo", "monospace"],
      },
      fontSize: {
        "2xs": ["10px", "14px"],
        xs: ["11px", "16px"],
        sm: ["12px", "18px"],
        base: ["13px", "20px"],
        md: ["14px", "20px"],
        lg: ["15px", "22px"],
        xl: ["18px", "26px"],
        "2xl": ["22px", "30px"],
        "3xl": ["28px", "36px"],
      },
      keyframes: {
        flash: {
          "0%": { backgroundColor: "rgba(0,212,170,0.25)" },
          "100%": { backgroundColor: "transparent" },
        },
        flashRed: {
          "0%": { backgroundColor: "rgba(240,94,110,0.25)" },
          "100%": { backgroundColor: "transparent" },
        },
        fadeIn: {
          "0%": { opacity: "0", transform: "translateY(4px)" },
          "100%": { opacity: "1", transform: "translateY(0)" },
        },
        pulse: {
          "0%, 100%": { opacity: "1" },
          "50%": { opacity: "0.4" },
        },
      },
      animation: {
        flash: "flash 0.9s ease-out forwards",
        "flash-red": "flashRed 0.9s ease-out forwards",
        "fade-in": "fadeIn 0.25s ease-out",
        pulse: "pulse 1.8s ease-in-out infinite",
      },
    },
  },
  plugins: [],
};

export default config;
