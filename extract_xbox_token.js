// Xbox Live Token Extractor
// Instructions:
// 1. Go to https://www.xbox.com and sign in
// 2. Open Developer Tools (F12 or Cmd+Option+I on Mac)
// 3. Go to Console tab
// 4. Paste this entire script and press Enter
// 5. Copy the token that appears

(async function() {
    console.log("🎮 Extracting Xbox Live Token...\n");

    // Try multiple methods to get the token
    const methods = [
        // Method 1: LocalStorage
        () => {
            const keys = Object.keys(localStorage);
            for (let key of keys) {
                const val = localStorage.getItem(key);
                if (val && val.includes('XBL3.0')) {
                    return val.match(/XBL3\.0[^"'\s]*/)?.[0];
                }
            }
            return null;
        },

        // Method 2: SessionStorage
        () => {
            const keys = Object.keys(sessionStorage);
            for (let key of keys) {
                const val = sessionStorage.getItem(key);
                if (val && val.includes('XBL3.0')) {
                    return val.match(/XBL3\.0[^"'\s]*/)?.[0];
                }
            }
            return null;
        },

        // Method 3: Make a request to Xbox API
        async () => {
            try {
                const response = await fetch('https://profile.xboxlive.com/users/me/profile/settings', {
                    credentials: 'include'
                });
                const authHeader = response.headers.get('authorization');
                if (authHeader) return authHeader;

                // Try to extract from cookies
                const xblToken = document.cookie.split('; ')
                    .find(row => row.startsWith('XBL'))
                    ?.split('=')[1];
                return xblToken;
            } catch (e) {
                console.log("API method failed:", e.message);
                return null;
            }
        }
    ];

    let token = null;
    for (let i = 0; i < methods.length; i++) {
        console.log(`Trying method ${i + 1}...`);
        token = await methods[i]();
        if (token) {
            console.log(`✓ Found token using method ${i + 1}\n`);
            break;
        }
    }

    if (token) {
        console.log("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");
        console.log("✅ SUCCESS! Your Xbox Live Token:");
        console.log("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");
        console.log(token);
        console.log("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");
        console.log("\nAdd this to your .env file:");
        console.log(`XBOX_ACCESS_TOKEN="${token}"`);
        console.log("\n📋 Token copied to clipboard!");

        // Copy to clipboard
        try {
            await navigator.clipboard.writeText(token);
        } catch (e) {
            console.log("⚠️  Couldn't auto-copy. Please copy the token above manually.");
        }
    } else {
        console.log("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");
        console.log("❌ Couldn't find token automatically");
        console.log("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");
        console.log("\nTry these steps:");
        console.log("1. Make sure you're signed into Xbox.com");
        console.log("2. Go to Application tab in DevTools");
        console.log("3. Look in Storage → Local Storage → xbox.com");
        console.log("4. Find any key containing 'token' or 'auth'");
        console.log("5. Look for a value starting with 'XBL3.0' or 'eyJ'");
    }
})();
