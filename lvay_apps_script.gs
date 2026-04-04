/**
 * LVAY - Recalculate & Publish Apps Script
 * ==========================================
 * Adds a custom "LVAY Tools" menu to Google Sheets with a
 * "Recalculate & Publish" button.
 *
 * HOW TO INSTALL:
 * 1. Open your LVAY Sports Data 2026 Google Sheet
 * 2. Click Extensions → Apps Script
 * 3. Delete any existing code
 * 4. Paste this entire file
 * 5. Click Save (floppy disk icon)
 * 6. Click Run → onOpen to initialize
 * 7. Authorize when prompted
 * 8. Refresh your Sheet — "LVAY Tools" menu appears at top
 */

const RENDER_BASE_URL = "https://lvay-scraper.onrender.com";

/**
 * Adds the LVAY Tools menu when the Sheet opens.
 */
function onOpen() {
  SpreadsheetApp.getUi()
    .createMenu("⚡ LVAY Tools")
    .addItem("🔄 Recalculate & Publish (Football)", "recalculateFootball")
    .addItem("🔄 Recalculate & Publish (Baseball)", "recalculateBaseball")
    .addItem("🔄 Recalculate & Publish (Softball)", "recalculateSoftball")
    .addSeparator()
    .addItem("🔄 Recalculate ALL Sports", "recalculateAll")
    .addSeparator()
    .addItem("✅ Validate Scores (Football)", "validateFootball")
    .addItem("📊 Export to WordPress", "exportToWordPress")
    .addSeparator()
    .addItem("ℹ️ Status Check", "checkStatus")
    .addToUi();
}

/**
 * Triggers football power rankings recalculation on Render.
 */
function recalculateFootball() {
  triggerEndpoint(
    "/api/build/football-sheets",
    "Football power rankings recalculating...\n\nCheck back in 3-5 minutes."
  );
}

function recalculateBaseball() {
  triggerEndpoint(
    "/api/rankings/calculate?sport=baseball",
    "Baseball power rankings recalculating...\n\nCheck back in 3-5 minutes."
  );
}

function recalculateSoftball() {
  triggerEndpoint(
    "/api/rankings/calculate?sport=softball",
    "Softball power rankings recalculating...\n\nCheck back in 3-5 minutes."
  );
}

function recalculateAll() {
  triggerEndpoint(
    "/api/rankings/calculate",
    "All sports recalculating...\n\nCheck back in 5-8 minutes."
  );
}

function validateFootball() {
  triggerEndpoint(
    "/api/validate/football",
    "Score validation running...\n\nCheck 'Football Needs Review' tab in 1-2 minutes."
  );
}

function exportToWordPress() {
  triggerEndpoint(
    "/api/export/wordpress",
    "Exporting to WordPress...\n\nYour LVAY pages will update in 2-3 minutes."
  );
}

/**
 * Check the scraper status.
 */
function checkStatus() {
  try {
    const response = UrlFetchApp.fetch(RENDER_BASE_URL + "/api/status");
    const data     = JSON.parse(response.getContentText());
    
    const records = data.records_by_sport || {};
    const message = [
      "✅ LVAY Scraper Status",
      "",
      `Baseball:  ${records.baseball || 0} games`,
      `Softball:  ${records.softball || 0} games`,
      `Football:  ${records.football || 0} games`,
      "",
      `Total: ${data.total_records || 0} records`,
      `Server time: ${data.server_time || "unknown"}`,
    ].join("\n");
    
    SpreadsheetApp.getUi().alert(message);
  } catch (e) {
    SpreadsheetApp.getUi().alert("Error checking status: " + e.message);
  }
}

/**
 * Generic function to trigger a Render API endpoint.
 */
function triggerEndpoint(path, successMessage) {
  const ui = SpreadsheetApp.getUi();
  
  try {
    const url      = RENDER_BASE_URL + path;
    const response = UrlFetchApp.fetch(url, {
      method: "GET",
      muteHttpExceptions: true,
    });
    
    const code = response.getResponseCode();
    if (code === 200) {
      ui.alert("⚡ LVAY Tools\n\n" + successMessage);
    } else {
      ui.alert("⚠️ Request sent but got status " + code + "\n\nThe scraper may be spinning up. Try again in 30 seconds.");
    }
  } catch (e) {
    ui.alert("❌ Error: " + e.message + "\n\nMake sure the scraper is running at:\n" + RENDER_BASE_URL);
  }
}

/**
 * Auto-runs daily at 6 AM Central to recalculate everything.
 * Set up via Triggers (clock icon in Apps Script editor).
 */
function scheduledRecalculate() {
  try {
    UrlFetchApp.fetch(RENDER_BASE_URL + "/api/rankings/calculate");
    console.log("Scheduled recalculation triggered at " + new Date());
  } catch (e) {
    console.error("Scheduled recalculation failed: " + e.message);
  }
}
