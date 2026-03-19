import type { Client as PgClient } from "npm:pg@^8.13.0"
import type { FlowcoreDataPumpCoordinator } from "../../src/data-pump/types.ts"

export class PostgresCoordinator implements FlowcoreDataPumpCoordinator {
  constructor(private readonly db: PgClient) {}

  async acquireLease(instanceId: string, key: string, ttlMs: number): Promise<boolean> {
    const expiresAt = new Date(Date.now() + ttlMs)
    const result = await this.db.query(
      `INSERT INTO flowcore_pump_leases (key, holder, expires_at)
       VALUES ($1, $2, $3)
       ON CONFLICT (key) DO UPDATE
         SET holder = $2, expires_at = $3
         WHERE flowcore_pump_leases.expires_at < NOW() OR flowcore_pump_leases.holder = $2
       RETURNING holder`,
      [key, instanceId, expiresAt],
    )
    return result.rowCount > 0 && result.rows[0].holder === instanceId
  }

  async renewLease(instanceId: string, key: string, ttlMs: number): Promise<boolean> {
    const expiresAt = new Date(Date.now() + ttlMs)
    const result = await this.db.query(
      `UPDATE flowcore_pump_leases SET expires_at = $3 WHERE key = $1 AND holder = $2`,
      [key, instanceId, expiresAt],
    )
    return (result.rowCount ?? 0) > 0
  }

  async releaseLease(instanceId: string, key: string): Promise<void> {
    await this.db.query(`DELETE FROM flowcore_pump_leases WHERE key = $1 AND holder = $2`, [key, instanceId])
  }

  async register(instanceId: string, address: string): Promise<void> {
    await this.db.query(
      `INSERT INTO flowcore_pump_instances (instance_id, address, last_heartbeat)
       VALUES ($1, $2, NOW())
       ON CONFLICT (instance_id) DO UPDATE SET address = $2, last_heartbeat = NOW()`,
      [instanceId, address],
    )
  }

  async heartbeat(instanceId: string): Promise<void> {
    await this.db.query(`UPDATE flowcore_pump_instances SET last_heartbeat = NOW() WHERE instance_id = $1`, [
      instanceId,
    ])
  }

  async unregister(instanceId: string): Promise<void> {
    await this.db.query(`DELETE FROM flowcore_pump_instances WHERE instance_id = $1`, [instanceId])
  }

  async getInstances(staleThresholdMs: number): Promise<Array<{ instanceId: string; address: string }>> {
    const threshold = new Date(Date.now() - staleThresholdMs)
    const result = await this.db.query(
      `SELECT instance_id, address FROM flowcore_pump_instances WHERE last_heartbeat > $1`,
      [threshold],
    )
    return result.rows.map((row: { instance_id: string; address: string }) => ({
      instanceId: row.instance_id,
      address: row.address,
    }))
  }
}
