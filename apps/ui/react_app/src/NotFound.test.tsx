import { render, screen } from "@testing-library/react";
import { axe } from "jest-axe";
import { MemoryRouter } from "react-router-dom";
import { describe, expect, it } from "vitest";
import { NotFound } from "./NotFound";

describe("not found route", () => {
  it("offers a clear route home without accessibility violations", async () => {
    const { container } = render(<MemoryRouter><NotFound /></MemoryRouter>);
    expect(screen.getByRole("heading", { name: "We could not find that page." })).toBeInTheDocument();
    expect(screen.getByRole("link", { name: "Return home" })).toHaveAttribute("href", "/");
    expect(await axe(container)).toHaveNoViolations();
  });
});
