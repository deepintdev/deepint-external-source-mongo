FROM node:alpine

RUN mkdir /app

WORKDIR /app

# Install dependencies

ADD package.json /app/package.json
ADD package-lock.json /app/package-lock.json

RUN npm install

# Add source files

ADD src /app/src 

# Build

ADD .eslintrc.js /app/.eslintrc.js
ADD tsconfig.json /app/tsconfig.json

RUN npm run build

# Remove dev dependencies

RUN npm prune --production

# Expose ports

EXPOSE 80
EXPOSE 443

# Entry point

CMD ["npm", "start"]