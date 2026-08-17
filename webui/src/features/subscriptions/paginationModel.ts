export function clampPage(page: number, total: number, pageSize: number): number {
  if (!Number.isInteger(page) || page < 0 || !Number.isInteger(pageSize) || pageSize < 1 || !Number.isFinite(total)) {
    return 0;
  }
  const totalPages = Math.max(1, Math.ceil(Math.max(0, total) / pageSize));
  return Math.min(page, totalPages - 1);
}

export function itemsForRender<T>(items: T[], isPlaceholderData: boolean): T[] {
  return isPlaceholderData ? [] : items;
}

export function dataForRender<T>(data: T | undefined, isPlaceholderData: boolean): T | undefined {
  return isPlaceholderData ? undefined : data;
}

// Kept as narrow aliases for callers that already use the subscription names.
export const clampSubscriptionPage = clampPage;
export const subscriptionItemsForRender = itemsForRender;
