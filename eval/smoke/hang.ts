// A model endpoint that accepts requests and never answers (or answers when HANG_MODE=ok). Logs each request.
const mode = () => Bun.file("/tmp/memex-smoke/hang.mode").text().then((s) => s.trim(), () => "hang");
Bun.serve({
  port: 18799, hostname: "0.0.0.0",
  async fetch(req) {
    const u = new URL(req.url);
    console.log(new Date().toISOString(), req.method, u.pathname);
    if ((await mode()) === "hang") return new Promise<Response>(() => {});
    if (u.pathname.endsWith("/embeddings")) {
      const b: any = await req.json();
      const n = Array.isArray(b.input) ? b.input.length : 1;
      return Response.json({ data: Array.from({ length: n }, (_, i) => ({ index: i, embedding: Array.from({ length: 1024 }, (_, k) => ((k * 7 + i) % 13) / 13) })) });
    }
    return Response.json({ choices: [{ message: { content: JSON.stringify({ summary: "s", user_asked: [], assistant_chose: [], verified: [], tasks: [], kind: ["chat"], anchors: [], statements: [] }) } }] });
  },
});
console.log("listening 18799");
