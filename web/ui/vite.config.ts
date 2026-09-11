import { defineConfig } from "vite";

export default defineConfig({
  build: {
    outDir: "../static/react",
    emptyOutDir: true,
    rollupOptions: {
      output: {
        entryFileNames: "librarr.js",
        chunkFileNames: "chunks/[name]-[hash].js",
        assetFileNames: (asset) =>
          asset.names?.some((name) => name.endsWith(".css"))
            ? "librarr.css"
            : "assets/[name]-[hash][extname]",
      },
    },
  },
});
