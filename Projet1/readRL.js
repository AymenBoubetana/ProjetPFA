import axios from "axios";
import fs from "fs";
import path from "path";
import { performance } from "perf_hooks";

const API_URL = "https://apidev.vengoreserve.com/api/view/allforms";
const SEARCH_TERM = "https://www.youtube.com/watch?v=1Snpg8Lsneg&list=RDfriFeafx8Y0&index=3";
const BEARER_TOKEN = "Access_token"; // replace with your token

async function fetchData() {
  const response = await axios.get(API_URL, {
    params: {
      type: "note,report",
      parent_form_id: 17914,
      with_template: 1,
      total: 600
    },
    headers: {
      Authorization: `Bearer ${BEARER_TOKEN}`
    }
  });

  // Check if response has 'data' field
  if (response.data && Array.isArray(response.data)) return response.data;
  if (response.data && Array.isArray(response.data.data)) return response.data.data;

  throw new Error("API response does not contain a valid data array");
}

(async () => {
  const startTime = performance.now();

  let allDocs = [];
  let matchCount = 0;

  const items = await fetchData();
  const totalScanned = items.length;

  for (const doc of items) {
    let found = false;
    if (Array.isArray(doc.decode_template)) {
      found = doc.decode_template.some(templateItem =>
        Array.isArray(templateItem?.sections) &&
        templateItem.sections.some(sectionItem =>
          typeof sectionItem?.field_value === "string" &&
          sectionItem.field_value.includes(SEARCH_TERM)
        )
      );
    }
    if (found) {
      allDocs.push(doc);
      matchCount++;
    }
  }

  const duration = (performance.now() - startTime) / 1000;
  console.log(`\n✅ Fetch completed in ${duration.toFixed(2)}s`);
  console.log(`📊 Total scanned: ${totalScanned}`);
  console.log(`🎯 Matches found: ${matchCount}`);

  if (allDocs.length > 0) {
    const filename = path.join(process.cwd(), `api_matches_${Date.now()}.json`);
    fs.writeFileSync(filename, JSON.stringify(allDocs, null, 2));
    console.log(`💾 Saved results to: ${filename}`);
  }
})();
