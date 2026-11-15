# Website

This website is built using [Docusaurus 2](https://docusaurus.io/), a modern
static website generator.

### Installation

```
$ yarn
```

### Local Development

```
$ yarn start
```

This command starts a local development server and opens up a browser window.
Most changes are reflected live without having to restart the server.

### Benchmark data

The benchmarks page fetches its data at runtime from `/benchmarks/data`, rather
than building it into the site. The benchmark workflow publishes this data to
the `gh-pages` branch. To view the page on a local development server, copy the
data into `static`:

```
$ git fetch origin gh-pages
$ git archive origin/gh-pages benchmarks/data | tar -x -C static
```

To add the results of a local JMH run instead, use `scripts/publishBenchmark.js`
with `--data-dir static/benchmarks/data`. Its header comment describes the
other options.

Delete `static/benchmarks` afterwards so that it is not committed.

### Build

```
$ yarn build
```

This command generates static content into the `build` directory and can be
served using any static contents hosting service.

### Deployment

Using SSH:

```
$ USE_SSH=true yarn deploy
```

Not using SSH:

```
$ GIT_USER=<Your GitHub username> yarn deploy
```

If you are using GitHub pages for hosting, this command is a convenient way to
build the website and push to the `gh-pages` branch.
