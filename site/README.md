# structured-data-streams.com

The website for Structured Data Streams (SDS): SQL, OLTP, OLAP, and filesystems as snapshots over an authoritative stream of proto records.

Built with [Hugo](https://gohugo.io/), deployed to GitHub Pages by `.github/workflows/site.yml`.

## Local Preview

Requires Hugo extended v0.165.0 or later.

```bash
cd site
make preview        # runs local server at http://localhost:1313/
make build          # builds the site into public/
```

## Structure

- `content/`: Markdown content pages (`idea.md`, `format.md`, `projections.md`, `case-study.md`, `faq.md`, `implementations.md`).
- `layouts/`: Hugo HTML templates (`baseof.html`, `home.html`, `page.html`, `404.html`, `_partials/`).
- `assets/css/`: Stylesheets (`site.css`, `landing.css`).
- `static/`: Static assets (favicon, images).

## Domains & Deployment

- Base URL defaults to `https://structured-data-streams.com/`.
- The GitHub Actions workflow (`.github/workflows/site.yml`) overrides `baseURL` with the GitHub Pages URL in CI/CD, enabling preview deployments on forks and branches.
- Once a custom domain is configured in Pages, a `CNAME` can be added to `static/CNAME`.
