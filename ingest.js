import { S3Client, GetObjectCommand } from "@aws-sdk/client-s3";
import pdf from "pdf-parse";
import fetch from "node-fetch";

const s3 = new S3Client({ region: "ap-south-1" });
const BUCKET = process.env.BUCKET_NAME;
const OPENAI_KEY = process.env.OPENAI_API_KEY;
const OPENSEARCH_URL = process.env.OPENSEARCH_URL;
const CHUNK_SIZE = parseInt(process.env.CHUNK_SIZE || "1500", 10);

function bufferFromStream(stream) {
  return new Promise((resolve, reject) => {
    const chunks = [];
    stream.on("data", (c) => chunks.push(c));
    stream.on("end", () => resolve(Buffer.concat(chunks)));
    stream.on("error", reject);
  });
}

function chunkText(text, size = CHUNK_SIZE) {
  const out = [];
  let i = 0;
  while (i < text.length) {
    out.push(text.slice(i, i + size));
    i += size;
  }
  return out;
}

export const handler = async (event) => {
  try {
    const body = event.body ? JSON.parse(event.body) : {};
    const { doc_id, s3_key } = body;

    if (!doc_id || !s3_key) {
      return { statusCode: 400, body: JSON.stringify({ error: "Missing doc_id or s3_key" }) };
    }

    // 1. Download PDF from S3
    const getObj = await s3.send(new GetObjectCommand({ Bucket: BUCKET, Key: s3_key }));
    const pdfBuffer = await bufferFromStream(getObj.Body);

    // 2. Extract text
    const data = await pdf(pdfBuffer);
    const fullText = data.text || "";

    // 3. Chunk text
    const chunks = chunkText(fullText);

    // 4. Generate embeddings
    const embeddings = [];
    for (const chunk of chunks) {
      const resp = await fetch("https://api.openai.com/v1/embeddings", {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
          Authorization: `Bearer ${OPENAI_KEY}`,
        },
        body: JSON.stringify({
          model: "text-embedding-3-small",
          input: chunk
        }),
      });

      const json = await resp.json();
      embeddings.push({
        text: chunk,
        vector: json.data[0].embedding
      });
    }

    // 5. Bulk upload to OpenSearch
    const bulkData = embeddings
      .map((e, i) => {
        const meta = { index: { _index: doc_id, _id: `${doc_id}-${i}` } };
        return JSON.stringify(meta) + "\n" + JSON.stringify({ text: e.text, embedding: e.vector });
      })
      .join("\n") + "\n";

    const bulkResp = await fetch(`${OPENSEARCH_URL}/_bulk`, {
      method: "POST",
      headers: { "Content-Type": "application/x-ndjson" },
      body: bulkData,
    });

    if (!bulkResp.ok) {
      throw new Error(await bulkResp.text());
    }

    return {
      statusCode: 200,
      body: JSON.stringify({ message: "ingest complete", chunks: embeddings.length }),
    };
  } catch (err) {
    console.error(err);
    return { statusCode: 500, body: JSON.stringify({ error: err.message }) };
  }
};
