import { Rate } from 'k6/metrics';

export const successRate = new Rate('success');

export function updateSuccessRate(resp) {
  if (resp.data[0][0] === null ) {
    successRate.add(false);
  } else {
    successRate.add(true);
  }
}