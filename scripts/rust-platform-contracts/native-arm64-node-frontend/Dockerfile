FROM node:24@sha256:0bab91d6a954a4a75de4a887edfda7386ed8711aa8b1eb5fb309ee0f0a2c544f AS frontend-dependencies
WORKDIR /dispatcharr-frontend
COPY frontend/package.json frontend/package-lock.json ./
RUN git init --quiet /tmp/dispatcharr-webworkify \
    && git -C /tmp/dispatcharr-webworkify fetch --quiet --depth=1 https://github.com/xqq/webworkify-webpack.git 24d1e719b4a6cac37a518b2bb10fe124527ef4ef \
    && test "$(git -C /tmp/dispatcharr-webworkify rev-parse FETCH_HEAD)" = 24d1e719b4a6cac37a518b2bb10fe124527ef4ef \
    && rm -rf /tmp/dispatcharr-webworkify
RUN GIT_CONFIG_COUNT=2 \
    GIT_CONFIG_KEY_0=url.https://github.com/xqq/webworkify-webpack.git.insteadOf \
    GIT_CONFIG_VALUE_0=ssh://git@github.com/xqq/webworkify-webpack.git \
    GIT_CONFIG_KEY_1=url.https://github.com/xqq/webworkify-webpack.git.insteadOf \
    GIT_CONFIG_VALUE_1=git@github.com:xqq/webworkify-webpack.git \
    npm ci --no-audit --no-fund
COPY frontend/ ./

FROM frontend-dependencies AS frontend
RUN --network=none npm run build
COPY LICENSE ATTRIBUTION.md /validation-credit/
USER 65532:65532
