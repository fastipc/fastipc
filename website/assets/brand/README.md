# FastIPC logo and brand assets

The logo of FastIPC, for the website, the README, the packages and anything that links to the project.
Everything is a vector file except `png/`; the SVGs contain the lettering as outlines, so they need no font.

## Files

`light` variants are for light backgrounds (dark lettering), `dark` variants for dark backgrounds (light lettering).
The orange parts are the same in both.

| File | What it is | Use it for |
|---|---|---|
| `fastipc-logo-light.svg`, `fastipc-logo-dark.svg` | The full logo: wordmark, tagline ("High-performance IPC") and icon | The README, title slides, anywhere with room |
| `fastipc-lockup-light.svg`, `fastipc-lockup-dark.svg` | Wordmark and icon, no tagline | The website's header, narrow spaces |
| `fastipc-wordmark-light.svg`, `fastipc-wordmark-dark.svg` | The word "FastIPC" alone | Where the icon is already shown nearby |
| `fastipc-icon.svg` | The icon alone, on a transparent background; works on light and dark | Inline marks |
| `fastipc-square.svg` | The icon in white on a rounded square with the orange gradient | The SVG favicon, app icons, social cards |
| `fastipc-avatar.svg` | The same square without rounded corners (sites that round avatars themselves) | Organization and profile avatars |

`png/` holds PNG exports of the same artwork:

| File | Size |
|---|---|
| `fastipc-logo-{light,dark}@2x.png`, `fastipc-lockup-{light,dark}@2x.png`, `fastipc-wordmark-{light,dark}@2x.png` | The SVGs at twice their nominal size |
| `fastipc-icon-1024.png` | The icon, 1024 px wide |
| `fastipc-square-512.png` | The rounded square, 512 × 512 (the website's social card image) |
| `fastipc-avatar-1024.png` | The avatar, 1024 × 1024 |
| `favicon-32.png` | The favicon, 32 × 32 |
| `apple-touch-icon-180.png` | The home-screen icon for iOS, 180 × 180 |

## Palette

| Role | Hex |
|---|---|
| Amber: speed lines, top node | `#EBA02F` |
| Orange: arcs | `#CD6B1C` |
| Deep orange: bottom node | `#CF601A` |
| Bronze gold: lower arrow | `#C88F2F` |
| Gold: bolt, sparkles | `#D9AA42` |
| Charcoal: wordmark on light | `#474A4E` |
| Slate: tagline on light | `#4E5155` |
| Off-white: wordmark on dark | `#ECEDEE` |
| Light grey: tagline on dark | `#C9CCCF` |
| Background, light | `#F0F6F6` |
| Square's gradient | `#EBA02F` → `#CF601A`, top left to bottom right |

The logo's oranges are too light for text on a light background. The website uses a darker orange for links and
text there (`--accent` in [`../site.css`](../site.css)), so they keep a contrast of at least 4.5:1.

## Type

- Wordmark: **Montserrat Bold Italic** (weight 700), tracking about -0.024 em.
- Tagline: **Montserrat Medium** (weight 500), upper case, letter-spaced to the wordmark's width.

Montserrat is a Google Font under the SIL Open Font License; the logo files contain outlines, not the font. The
website's own text uses other fonts.
