import './globals.css';

export const metadata = { title: 'RutaSur', description: 'Seguimiento de envíos en tiempo real' };

export default function RootLayout({ children }) {
  return (
    <html lang="es">
      <body>
        <nav>
          <strong>RutaSur</strong>
          <a href="/">Torre de control</a>
          <a href="/analitica">Analítica</a>
          <a href="/demo">Demo</a>
        </nav>
        <main>{children}</main>
      </body>
    </html>
  );
}
