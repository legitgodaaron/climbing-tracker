/* ClimbTracker service worker — conservative: caches static assets only.
   It deliberately does NOT intercept page navigations. Intercepting
   navigations on iOS standalone PWAs is a well-known source of blank
   pages / "dead" links, so we let the browser handle all navigation
   normally and only speed up same-origin static assets. */
const VERSION      = 'climbtracker-v2';
const STATIC_CACHE = `${VERSION}-static`;

// Static assets worth precaching. Each is added individually so one bad
// URL can never fail the whole install (and brick the worker).
const PRECACHE = [
  '/static/manifest.webmanifest',
  '/static/icons/icon-192.png',
  '/static/icons/icon-512.png',
  '/static/icons/icon-maskable-512.png',
  '/static/icons/apple-touch-icon.png',
];

self.addEventListener('install', (event) => {
  event.waitUntil(
    caches.open(STATIC_CACHE).then((cache) =>
      Promise.all(PRECACHE.map((url) => cache.add(url).catch(() => null)))
    ).then(() => self.skipWaiting())
  );
});

self.addEventListener('activate', (event) => {
  event.waitUntil(
    caches.keys().then((keys) =>
      Promise.all(
        keys.filter((k) => !k.startsWith(VERSION)).map((k) => caches.delete(k))
      )
    ).then(() => self.clients.claim())
  );
});

self.addEventListener('fetch', (event) => {
  const { request } = event;
  if (request.method !== 'GET') return;

  const url = new URL(request.url);

  // Never touch navigations or cross-origin requests — let the network handle them.
  if (request.mode === 'navigate') return;
  if (url.origin !== self.location.origin) return;

  // Only same-origin static assets: cache-first, then network (and cache it).
  if (url.pathname.startsWith('/static/')) {
    event.respondWith(
      caches.match(request).then((cached) => {
        if (cached) return cached;
        return fetch(request).then((response) => {
          if (response && response.ok) {
            const copy = response.clone();
            caches.open(STATIC_CACHE).then((cache) => cache.put(request, copy));
          }
          return response;
        });
      })
    );
  }
});
