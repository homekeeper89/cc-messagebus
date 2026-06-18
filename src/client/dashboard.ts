const DEFAULT_BASE_URL = "http://127.0.0.1:5959";

export interface RunDashboardOptions {
	baseUrl?: string;
}

export function runDashboard(opts: RunDashboardOptions = {}): void {
	const baseUrl = opts.baseUrl ?? DEFAULT_BASE_URL;
	process.stdout.write(`Dashboard: ${baseUrl}/dashboard\n`);
}
