import type { NextConfig } from 'next';
import createNextIntlPlugin from 'next-intl/plugin';

const withNextIntl = createNextIntlPlugin();

const nextConfig: NextConfig = {
  async redirects() {
    return [
      {
        source: '/leagues',
        destination: '/competitions',
        permanent: true,
      },
      {
        source: '/leagues/:path*',
        destination: '/competitions/:path*',
        permanent: true,
      },
    ];
  },
};

export default withNextIntl(nextConfig);
