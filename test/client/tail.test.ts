import assert from "node:assert/strict";
import { describe, test } from "node:test";
import { parseSseChunks } from "../../src/client/tail.js";

describe("parseSseChunks", () => {
	test("splits on double newline", () => {
		const input =
			'event: message_delivered\ndata: {"type":"message_delivered","message":{"id":"m1"}}\n\nevent: message_delivered\ndata: {"type":"message_delivered","message":{"id":"m2"}}\n\n';
		const { events, rest } = parseSseChunks(input);
		assert.equal(events.length, 2);
		assert.deepEqual(events[0], {
			type: "message_delivered",
			message: { id: "m1" },
		});
		assert.deepEqual(events[1], {
			type: "message_delivered",
			message: { id: "m2" },
		});
		assert.equal(rest, "");
	});

	test("preserves incomplete trailing chunk across boundary", () => {
		const input =
			'event: message_delivered\ndata: {"type":"message_delivered","message":{"id":"m1"}}\n\nevent: message_delivered\ndata: {"type":"message_delivered","mes';
		const { events, rest } = parseSseChunks(input);
		assert.equal(events.length, 1);
		assert.deepEqual(events[0], {
			type: "message_delivered",
			message: { id: "m1" },
		});
		assert.equal(
			rest,
			'event: message_delivered\ndata: {"type":"message_delivered","mes',
		);
	});

	test("ignores comment lines starting with colon", () => {
		const input =
			': keep-alive\nevent: message_delivered\ndata: {"type":"message_delivered","message":{"id":"m1"}}\n\n';
		const { events, rest } = parseSseChunks(input);
		assert.equal(events.length, 1);
		assert.deepEqual(events[0], {
			type: "message_delivered",
			message: { id: "m1" },
		});
		assert.equal(rest, "");
	});

	test("silently drops malformed json payloads", () => {
		const input =
			'event: message_delivered\ndata: {malformed\n\nevent: message_delivered\ndata: {"type":"message_delivered","message":{"id":"m2"}}\n\n';
		const { events, rest } = parseSseChunks(input);
		assert.equal(events.length, 1, "malformed event dropped, valid one kept");
		assert.deepEqual(events[0], {
			type: "message_delivered",
			message: { id: "m2" },
		});
		assert.equal(rest, "");
	});
});
