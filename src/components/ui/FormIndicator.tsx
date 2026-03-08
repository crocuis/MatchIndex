import { cn } from '@/lib/utils';
import { getFormColor } from '@/lib/utils';

interface FormIndicatorProps {
  form: ('W' | 'D' | 'L')[];
  className?: string;
}

export function FormIndicator({ form, className }: FormIndicatorProps) {
  return (
    <div className={cn('flex items-center gap-1', className)}>
      {form.map((result, i) => (
        <div
          key={i}
          className={cn(
            'h-4 w-4 rounded-sm flex items-center justify-center text-[9px] font-bold text-white',
            getFormColor(result)
          )}
          title={result === 'W' ? 'Win' : result === 'D' ? 'Draw' : 'Loss'}
        >
          {result}
        </div>
      ))}
    </div>
  );
}
