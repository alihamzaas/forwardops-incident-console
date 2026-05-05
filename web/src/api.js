const JSON_HEADERS = { "Content-Type": "application/json" };

export async function getHealth() {
  return getJson("/api/copilot/health");
}

export async function getGraphShape() {
  return getJson("/api/copilot/graph");
}

export async function loadSampleDataset() {
  return postJson("/api/copilot/sample", {});
}

export async function uploadDataset(file) {
  const body = new FormData();
  body.append("file", file);
  const response = await fetch("/api/copilot/upload", {
    method: "POST",
    body,
  });
  return parseResponse(response);
}

export async function streamAnalysis({ sessionId, query, onEvent }) {
  const response = await fetch("/api/copilot/analyze", {
    method: "POST",
    headers: JSON_HEADERS,
    body: JSON.stringify({ session_id: sessionId, query }),
  });
  if (!response.ok || !response.body) {
    throw new Error(await response.text());
  }

  const reader = response.body.getReader();
  const decoder = new TextDecoder();
  let buffer = "";

  while (true) {
    const { value, done } = await reader.read();
    if (done) {
      break;
    }
    buffer += decoder.decode(value, { stream: true });
    const packets = buffer.split("\n\n");
    buffer = packets.pop() || "";
    for (const packet of packets) {
      const parsed = parseSsePacket(packet);
      if (parsed) {
        onEvent(parsed);
      }
    }
  }
}

async function getJson(path) {
  const response = await fetch(path);
  return parseResponse(response);
}

async function postJson(path, body) {
  const response = await fetch(path, {
    method: "POST",
    headers: JSON_HEADERS,
    body: JSON.stringify(body),
  });
  return parseResponse(response);
}

async function parseResponse(response) {
  if (!response.ok) {
    throw new Error(await response.text());
  }
  return response.json();
}

function parseSsePacket(packet) {
  const event = packet.match(/^event:\s*(.+)$/m)?.[1] || "message";
  const data = packet
    .split("\n")
    .filter((line) => line.startsWith("data:"))
    .map((line) => line.replace(/^data:\s?/, ""))
    .join("\n");
  if (!data) {
    return null;
  }
  return { event, payload: JSON.parse(data) };
}
