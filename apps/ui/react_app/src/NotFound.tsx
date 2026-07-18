import { ArrowLeft, Compass } from "lucide-react";
import { Link } from "react-router-dom";

export function NotFound() {
  return <main className="not-found" id="main-content">
    <Link to="/" className="brand"><i className="brand-mark" /><span>Sceptre <b>AI</b></span></Link>
    <section>
      <span className="not-found__code">404</span>
      <Compass aria-hidden />
      <p className="eyebrow">This route is not part of the workspace</p>
      <h1>We could not find that page.</h1>
      <p>The address may be outdated, or the project link may no longer be available.</p>
      <Link className="button button--primary" to="/"><ArrowLeft size={16} />Return home</Link>
    </section>
  </main>;
}
