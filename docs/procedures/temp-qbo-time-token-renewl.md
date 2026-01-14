# QBO Time — Token Renewal Procedure

**Frequency**: When expire (currently set to year-end)  
**Audience**: Internal / Engineer (Auth + Data Platform)

---

1. **Open Token File**
    - Open `token.json` for **QBO Time**
2. **Verify Company Context**

    For **each location**:
    - Open the **URL**
    - Double-check the **Company Name**
    > ⚠️ Ensure you are modifying the correct company before proceeding.

3. **Navigate to API Settings** on QBO Time UI
    - In the **left panel**:
        - Click **Feature Add-ons**
        - Select **API**
4. **Validate OAuth Credentials**

    Double-check the following fields:
    - **OAuth Client ID**
    - **OAuth Client Secret**
    - Token
5. **Extend Token Expiration**
    - Extend the **expiration date**
    - Save changes