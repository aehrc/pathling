export const catchError = (message, opOutcome) => ({
  type: 'CATCH_ERROR',
  message,
  opOutcome,
})
