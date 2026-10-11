// A model endpoint on :18799 that accepts requests and never answers, or (when /tmp/memex-smoke/hang.mode holds
// "ok") answers each prompt kind with a minimal valid reply. Logs each request with the prompt kind.
const mode = () => Bun.file("/tmp/memex-smoke/hang.mode").text().then((s) => s.trim(), () => "hang");
Bun.serve({
  port: 18799, hostname: "0.0.0.0",
  async fetch(req) {
    const u = new URL(req.url);
    const b: any = await req.json().catch(() => ({}));
    const sys: string = b.messages?.[0]?.content ?? "";
    console.log(new Date().toISOString(), req.method, u.pathname, sys.slice(0, 20));
    if ((await mode()) === "hang") return new Promise<Response>(() => {});
    if (u.pathname.endsWith("/embeddings")) {
      const n = Array.isArray(b.input) ? b.input.length : 1;
      return Response.json({ data: Array.from({ length: n }, (_, i) => ({ index: i, embedding: Array.from({ length: 1024 }, (_, k) => ((k * 7 + i) % 13) / 13) })) });
    }
    const reply = sys.startsWith("You extract") ? { statements: [] }
      : sys.startsWith("You write the card") ? { title: "T", overview: "card", results: [], open_threads: [] }
      : { summary: "smoke summary", user_asked: [], assistant_chose: [], verified: [], tasks: [], kind: ["chat"], anchors: [] };
    return Response.json({ choices: [{ message: { content: JSON.stringify(reply) } }] });
  },
});
console.log("listening 18799");
