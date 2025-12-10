# Migration Guide: React Component to Static Site

This guide details the procedure to convert the existing `join_enumerator.jsx` React component into a standalone, static HTML/JS web page using Vite.

## Prerequisites
- **Node.js** and **npm** installed.
- Terminal access.

## Step 1: Initialize Project
Create a new Vite project to serve as the "harness" for the React code.

```bash
# Create project (select React + JavaScript when prompted, or use template)
npm create vite@latest join-enumerator-static -- --template react

# Enter directory
cd join-enumerator-static

# Install dependencies
npm install
```

## Step 2: Configure Tailwind CSS
The current component uses Tailwind classes (`className="bg-purple-600..."`), so Tailwind must be configured.

```bash
# Install Tailwind and its peer dependencies
npm install -D tailwindcss postcss autoprefixer

# Initialize tailwind.config.js
npx tailwindcss init -p
```

**Update `tailwind.config.js`:**
Configure the content paths to include your source files:
```javascript
/** @type {import('tailwindcss').Config} */
export default {
  content: [
    "./index.html",
    "./src/**/*.{js,ts,jsx,tsx}",
  ],
  theme: {
    extend: {},
  },
  plugins: [],
}
```

**Update `src/index.css`:**
Replace the contents with Tailwind directives:
```css
@tailwind base;
@tailwind components;
@tailwind utilities;
```

## Step 3: Migrate Code
Move the existing logic into the new project structure.

1.  **Copy the Component**:
    Copy `join_enumerator.jsx` from your current workspace to `join-enumerator-static/src/JoinEnumeratorApp.jsx`.

2.  **Update Entry Point (`src/App.jsx`)**:
    Replace the default Vite App code with your component:
    ```jsx
    import JoinEnumeratorApp from './JoinEnumeratorApp'

    function App() {
      return (
        <JoinEnumeratorApp />
      )
    }

    export default App
    ```

3.  **Fix Imports**:
    Ensure `JoinEnumeratorApp.jsx` imports React correctly:
    ```javascript
    import React, { useState } from 'react';
    ```

## Step 4: Build for Production
Generate the static files.

```bash
npm run build
```

- This creates a `dist/` directory.
- `dist/index.html` is the entry point.
- All CSS and JS are bundled and linked automatically.

## Step 5: Verification
Preview the static build locally to ensure it works before sharing.

```bash
npm run preview
```

## Step 6: Sharing
- Zip the contents of the `dist/` folder.
- Or host the `dist/` folder on GitHub Pages, Netlify, or Vercel.
- The `index.html` file acts as the standalone static page.
