import type { Metadata } from 'next';
import { Geist, Geist_Mono } from 'next/font/google';
import { NextIntlClientProvider } from 'next-intl';
import { getLocale, getMessages } from 'next-intl/server';
import { Sidebar } from '@/components/layout/Sidebar';
import { TopBar } from '@/components/layout/TopBar';
import { APP_VERSION } from '@/config/app';
import { getTeamTranslationSnapshotDb } from '@/data/server';
import './globals.css';

const geistSans = Geist({
  variable: '--font-geist-sans',
  subsets: ['latin'],
});

const geistMono = Geist_Mono({
  variable: '--font-geist-mono',
  subsets: ['latin'],
});

export const metadata: Metadata = {
  title: { template: '%s | MatchIndex', default: 'MatchIndex' },
  description: 'Football data exploration platform',
};

export default async function RootLayout({
  children,
}: Readonly<{
  children: React.ReactNode;
}>) {
  const locale = await getLocale();
  const messages = await getMessages();
  const hasDatabase = Boolean(process.env.DATABASE_URL);

  if (hasDatabase) {
    await getTeamTranslationSnapshotDb(locale);
  }

  return (
    <html lang={locale} className="dark">
      <body className={`${geistSans.variable} ${geistMono.variable} antialiased`}>
        <NextIntlClientProvider messages={messages}>
          <div className="flex h-screen flex-col overflow-hidden bg-surface-0 font-sans text-[13px]">
            <TopBar />
            <div className="flex flex-1 overflow-hidden">
              <Sidebar appVersion={APP_VERSION} hasDatabase={hasDatabase} />
              <main className="flex-1 overflow-y-auto p-4 md:p-6 bg-surface-0">
                <div className="mx-auto max-w-[1400px]">
                  {children}
                </div>
              </main>
            </div>
          </div>
        </NextIntlClientProvider>
      </body>
    </html>
  );
}
