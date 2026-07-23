import { fireEvent, render, screen } from "@testing-library/react";
import { axe } from "jest-axe";
import { describe, expect, it, vi } from "vitest";
import { Badge, Loading, Modal, Notice } from "./ui";

describe("shared UI primitives", () => {
  it("renders semantic status and notice content without accessibility violations", async () => {
    const { container } = render(<><Badge status="succeeded" /><Notice tone="danger">Failed safely</Notice></>);
    expect(screen.getByText("Succeeded")).toBeInTheDocument();
    expect(screen.getByRole("alert")).toHaveTextContent("Failed safely");
    expect(await axe(container)).toHaveNoViolations();
  });

  it("closes a modal with Escape", () => {
    const close = vi.fn();
    render(<Modal title="Confirm deletion" onClose={close}><p>Content</p></Modal>);
    expect(screen.getByRole("dialog")).toHaveAttribute("aria-modal", "true");
    expect(screen.getByRole("button", { name: "Close" })).toHaveFocus();
    fireEvent.keyDown(document, { key: "Escape" });
    expect(close).toHaveBeenCalledOnce();
  });

  it("keeps keyboard focus inside a modal", () => {
    render(<Modal title="Edit project" onClose={() => undefined}><button>Save project</button></Modal>);
    const close = screen.getByRole("button", { name: "Close" });
    const save = screen.getByRole("button", { name: "Save project" });
    expect(close).toHaveFocus();
    fireEvent.keyDown(document, { key: "Tab", shiftKey: true });
    expect(save).toHaveFocus();
    fireEvent.keyDown(document, { key: "Tab" });
    expect(close).toHaveFocus();
  });

  it("announces skeleton loading states", () => {
    render(<Loading label="Preparing project evidence…" />);
    expect(screen.getByRole("status")).toHaveTextContent("Preparing project evidence…");
  });
});
