import type { Client as PgClient } from "npm:pg@^8.13.0"
import type { FlowcoreDataPumpState, FlowcoreDataPumpStateManager } from "../../src/data-pump/types.ts"

export class PostgresStateManager implements FlowcoreDataPumpStateManager {
  constructor(private readonly db: PgClient) {}

  async getState(): Promise<FlowcoreDataPumpState | null> {
    const result = await this.db.query(
      `SELECT time_bucket, event_id FROM pump_state ORDER BY updated_at DESC LIMIT 1`,
    )
    if (result.rows.length === 0) return null
    return {
      timeBucket: result.rows[0].time_bucket,
      eventId: result.rows[0].event_id,
    }
  }

  async setState(state: FlowcoreDataPumpState): Promise<void> {
    await this.db.query(
      `INSERT INTO pump_state (id, time_bucket, event_id, updated_at)
       VALUES ('default', $1, $2, NOW())
       ON CONFLICT (id) DO UPDATE SET time_bucket = $1, event_id = $2, updated_at = NOW()`,
      [state.timeBucket, state.eventId],
    )
  }
}
