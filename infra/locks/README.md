# Runtime dependency locks (task-index P8-W02).
#
# Regenerate with:
#   uv pip compile requirements-<runtime>.txt \
#     -o infra/locks/requirements-<runtime>.lock \
#     --generate-hashes --no-annotate
#
# Install inside images with:
#   python -m pip install --require-hashes -r infra/locks/requirements-<runtime>.lock
#
# Known exception: Dockerfile.training.cpu installs torch==2.9.1 from the
# PyTorch CPU index (https://download.pytorch.org/whl/cpu). That wheel is not
# on PyPI, so its digest cannot appear in requirements-training.lock. The pin
# is exact (torch==2.9.1); revisit if the CPU index publishes verifiable
# per-wheel digests compatible with pip --require-hashes.
