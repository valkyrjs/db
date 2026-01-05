/**
 * Generate a number from the given condition and option combination.
 *
 * @param condition - Condition to hash.
 * @param options   - Options to hash.
 */
export function hashCodeQuery(condition: unknown, options: unknown): number {
  const value = JSON.stringify({ condition, options });
  let hash = 0;
  if (value.length === 0) {
    return hash;
  }
  for (let i = 0; i < value.length; i += 1) {
    hash = (hash << 5) - hash + value.charCodeAt(i);
    hash |= 0;
  }
  return hash;
}
