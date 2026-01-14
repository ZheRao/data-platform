# QBO — Token Renewal Procedure

**Frequency**: Quarterly  
**Audience**: Internal / Engineer (Auth + Data Platform)

---

1. **Load Client Secrets**
    - Open `client_secrets.json`
    - Locate and use the `redirect_url`
2. **Prepare Tokens File**
    - Open `tokens.json` for **QBO**
    - Keep it open for editing
3. **Select Workspace & App (Website)**

    On the Intuit developer site:
    - Select **Workspace** and **App**
    - Verify **client_id** and **client_secret**  
    
    **Workspace selection**:
    - Canada:
        - `Sample Workspace` + `Canada Data (Production)`
    - US:
        - `Sample Workspace` + `US Data (Production)`
4. **Generate Authorization Code**
    - Under **OAuth settings**:
        - Select `com.intuit.quickbooks.accounting`
        - Click **Get authorized code**
5. **Authorize Individual Locations**

    The page will redirect to location selection.
    - **CA**
        - Monette Farms BC Ltd.
        - Monette Farms Ltd.
        - Monette Produce Ltd.
        - Monette Seeds Ltd.
        - NexGen Seeds Ltd.
    - **US**
        - Monette Farms Arizona LLC
        - Monette Farms USA, Inc.
        - Monette Produce, LLC
        - Monette Seeds USA, LLC
6. ***repeat***: Steps for Each Location
    
    **Verify Realm ID**
    - After selecting a location:
        - Redirect back to the previous page
        - Compare **Realm ID** with the value in `tokens.json`
        - Confirm correctness **before editing**
    
    **Retrieve Tokens**
    - Click **Get tokens**

    **Update** `refresh_token`
    - In `tokens.json`:
        - **Delete** the existing `refresh_token`
        - **Replace** it with the newly generated value from the website


# Important Notes
- Only `refresh_token` is required for:
    - Automated auth refresh
    - Data extraction pipelines
- Updating `access_token` is **optional**
    - (Safe to do if desired, but not required)

# Final Validation Step

**Immediately perform a data extraction run**  
to confirm:
- Tokens are valid
- Auth refresh is functioning correctly
- No downstream failures occur






