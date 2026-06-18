import assert from "node:assert/strict";
import { describe, test } from "node:test";
import { parseSseChunks } from "../../src/client/tail.js";

describe("parseSseChunks", () => {
	test("splits on double newline", () => {
		const input =
			'event: heartbeat\ndata: {"type":"heartbeat","at":"t1"}\n\nevent: message_delivered\ndata: {"type":"message_delivered","message":{"id":"m1"}}\n\n';
		const { events, rest } = parseSseChunks(input);
		assert.equal(events.length, 2);
		assert.deepEqual(events[0], { type: "heartbeat", at: "t1" });
		assert.deepEqual(events[1], {
			type: "message_delivered",
			message: { id: "m1" },
		});
		assert.equal(rest, "");
	});

	test("preserves incomplete trailing chunk across boundary", () => {
		const input =
			'event: heartbeat\ndata: {"type":"heartbeat","at":"t1"}\n\nevent: message_delivered\ndata: {"type":"message_delivered","mes';
		const { events, rest } = parseSseChunks(input);
		assert.equal(events.length, 1);
		assert.deepEqual(events[0], { type: "heartbeat", at: "t1" });
		assert.equal(
			rest,
			'event: message_delivered\ndata: {"type":"message_delivered","mes',
		);
	});

	test("ignores comment lines starting with colon", () => {
		const input =
			': keep-alive\nevent: heartbeat\ndata: {"type":"heartbeat","at":"t1"}\n\n';
		const { events, rest } = parseSseChunks(input);
		assert.equal(events.length, 1);
		assert.deepEqual(events[0], { type: "heartbeat", at: "t1" });
		assert.equal(rest, "");
	});

	test("silently drops malformed json payloads", () => {
		const input =
			'event: heartbeat\ndata: {malformed\n\nevent: heartbeat\ndata: {"type":"heartbeat","at":"t2"}\n\n';
		const { events, rest } = parseSseChunks(input);
		assert.equal(events.length, 1, "malformed event dropped, valid one kept");
		assert.deepEqual(events[0], { type: "heartbeat", at: "t2" });
		assert.equal(rest, "");
	});
});
