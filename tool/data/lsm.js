// TODO(peter):
//
// - interactions
//   - mouse wheel: horizontal zoom
//   - click/drag: horizontal pan

// The heights of each level. The first few levels are given smaller
// heights to account for the increasing target file size.
//
// TODO(peter): Use the TargetFileSizes specified in the OPTIONS file.
const levelHeights = [16, 16, 16, 16, 32, 64, 128];
const levelOffsets = levelHeights.map((v, i) =>
    levelHeights.slice(0, i + 1).reduce((sum, elem) => sum + elem, 24)
);
const lineStart = 105;
var levelWidth = 0;

{
    // Create the base DOM elements.
    var c = d3
        .select("body")
        .append("div")
        .attr("id", "container");
    var h = c.append("div").attr("id", "header");
    h
        .append("div")
        .attr("class", "index")
        .append("input")
        .attr("type", "text")
        .attr("id", "index")
        .attr("autocomplete", "off");
    h.append("svg").attr("id", "slider");
    c.append("svg").attr("id", "vis");
}

var vis = d3.select("#vis");
vis
    .append("text")
    .attr("class", "help")
    .attr("x", 10)
    .attr("y", levelOffsets[6] + 30)
    .text(
        "(space: start/stop, left-arrow[+shift]: step-back, right-arrow[+shift]: step-forward)"
    );

var reason = vis
    .append("text")
    .attr("class", "reason")
    .attr("x", 10)
    .attr("y", 16);

var index = d3.select("#index");

// Pretty formatting of a number in human readable units.
function humanize(s) {
    const iecSuffixes = [" B", " KB", " MB", " GB", " TB", " PB", " EB"];
    if (s < 10) {
        return "" + s;
    }
    var e = Math.floor(Math.log(s) / Math.log(1024));
    var suffix = iecSuffixes[Math.floor(e)];
    var val = Math.floor(s / Math.pow(1024, e) * 10 + 0.5) / 10;
    return val.toFixed(val < 10 ? 1 : 0) + suffix;
}

function styleWidth(e) {
    var width = +e.style("width").slice(0, -2);
    return Math.round(Number(width));
}

function styleHeight(e) {
    var height = +e.style("height").slice(0, -2);
    return Math.round(Number(height));
}

var sliderX, sliderHandle;

// The version object holds the current LSM state.
var version = {
    levels: [[], [], [], [], [], [], []],
    // The version edit index.
    index: -1,

    // Set the version edit index. This steps either forward or
    // backward through the version edits, applying or unapplying each
    // edit.
    set: function(index) {
        if (index < 0) {
            index = 0;
        } else if (index >= data.Edits.length) {
            index = data.Edits.length - 1;
        }
        if (index == this.index) {
            return;
        }

        // If the current edit index is less than the target index,
        // step forward applying edits.
        for (; this.index < index; this.index++) {
            var edit = data.Edits[this.index + 1];
            for (var level in edit.Deleted) {
                this.remove(level, edit.Deleted[level]);
            }
            for (var level in edit.Added) {
                this.add(level, edit.Added[level]);
            }
        }

        // If the current edit index is greater than the target index,
        // step backward unapplying edits.
        for (; this.index > index; this.index--) {
            var edit = data.Edits[this.index];
            for (var level in edit.Added) {
                this.remove(level, edit.Added[level]);
            }
            for (var level in edit.Deleted) {
                this.add(level, edit.Deleted[level]);
            }
        }

        // Sort the levels.
        for (var i in this.levels) {
            if (i == 0) {
                this.levels[i].sort(function(a, b) {
                    var fa = data.Files[a];
                    var fb = data.Files[b];
                    if (fa.LargestSeqNum < fb.LargestSeqNum) {
                        return -1;
                    }
                    if (fa.LargestSeqNum > fb.LargestSeqNum) {
                        return +1;
                    }
                    if (fa.SmallestSeqNum < fb.SmallestSeqNum) {
                        return -1;
                    }
                    if (fa.SmallestSeqNum > fb.SmallestSeqNum) {
                        return +1;
                    }
                    return a < b;
                });
            } else {
                this.levels[i].sort(function(a, b) {
                    var fa = data.Files[a];
                    var fb = data.Files[b];
                    if (fa.Smallest < fb.Smallest) {
                        return -1;
                    }
                    if (fa.Smallest > fb.Smallest) {
                        return +1;
                    }
                    return 0;
                });
            }
        }

        this.render();
    },

    // Add the specified sstables to the specifed level.
    add: function(level, fileNums) {
        for (var i = 0; i < fileNums.length; i++) {
            this.levels[level].push(fileNums[i]);
        }
    },

    // Remove the specified sstables from the specifed level.
    remove: function(level, fileNums) {
        var l = this.levels[level];
        for (var i = 0; i < l.length; i++) {
            if (fileNums.indexOf(l[i]) != -1) {
                l[i] = l[l.length - 1];
                l.pop();
                i--;
            }
        }
    },

    // Return the size of the sstables in a level.
    size: function(level) {
        return this.levels[level].reduce(
            (sum, elem) => sum + data.Files[elem].Size,
            0
        );
    },

    // Returns the height to use for an sstable.
    height: function(fileNum) {
        var meta = data.Files[fileNum];
        return Math.ceil((meta.Size + 1024.0 * 1024.0 - 1) / (1024.0 * 1024.0));
    },

    scale: function(level) {
        return levelWidth < this.levels[level].length
            ? levelWidth / this.levels[level].length
            : 1;
    },

    // Return a summary of the count and size of the specified sstables.
    summarize: function(level, fileNums) {
        var count = 0;
        var size = 0;
        for (var fileNum of fileNums) {
            count++;
            size += data.Files[fileNum].Size;
        }
        return count + " @ " + "L" + level + " (" + humanize(size) + ")";
    },

    // Return a textual description of a version edit.
    describe: function(edit) {
        var s = edit.Reason;

        if (edit.Deleted) {
            var sep = " ";
            for (var i = 0; i < 7; i++) {
                if (edit.Deleted[i]) {
                    s += sep + this.summarize(i, edit.Deleted[i]);
                    sep = " + ";
                }
            }
        }

        if (edit.Added) {
            var sep = " => ";
            for (var i = 0; i < 7; i++) {
                if (edit.Added[i]) {
                    s += sep + this.summarize(i, edit.Added[i]);
                    sep = " + ";
                }
            }
        }

        return s;
    },

    render: function() {
        var version = this;

        vis.interrupt();

        // Render the edit info.
        var info = "[" + this.describe(data.Edits[this.index]) + "]";
        reason.text(info);

        // Render the text for each level: sstable count and size.
        vis
            .selectAll("text.levels")
            .data(this.levels)
            .enter()
            .append("text")
            .attr("class", "levels")
            .attr("x", 10)
            .attr("y", (d, i) => levelOffsets[i])
            .text((d, i) => "L" + i);
        vis
            .selectAll("text.counts")
            .data(this.levels)
            .text((d, i) => d.length)
            .enter()
            .append("text")
            .attr("class", "counts")
            .attr("text-anchor", "end")
            .attr("x", 55)
            .attr("y", (d, i) => levelOffsets[i])
            .text((d, i) => d.length);
        vis
            .selectAll("text.sizes")
            .data(this.levels)
            .text((d, i) => humanize(version.size(i)))
            .enter()
            .append("text")
            .attr("class", "sizes")
            .attr("text-anchor", "end")
            .attr("x", 100)
            .attr("y", (d, i) => levelOffsets[i])
            .text((d, i) => humanize(version.size(i)));

        // Render each of the levels. Each level is composed of an
        // outer group which provides a clipping recentangle, an inner
        // group defining the coordinate system, an overlap rectangle
        // to capture mouse events, an indicator rectangle used to
        // display sstable overlaps, and the per-sstable rectangles.
        var indicators = [];
        for (var i in this.levels) {
            var g = vis
                .selectAll("g.clip" + i)
                .select("g")
                .data([i]);
            var clipG = g
                .enter()
                .append("g")
                .attr("class", "clip" + i)
                .attr("clip-path", "url(#L" + i + ")");
            clipG
                .append("g")
                .attr(
                    "transform",
                    "translate(" +
                        lineStart +
                        "," +
                        levelOffsets[i] +
                        ") scale(1,-1)"
                );

            // TODO(peter): Why do closures capture the wrong variable
            // if this is declared as "var indicators =". It is as-if
            // there is only a single variable defined outside of the
            // loop.
            indicators[i] = clipG.append("rect").attr("class", "indicator");

            // Define the overlap rectangle for capturing mouse events.
            clipG
                .append("rect")
                .attr("x", lineStart)
                .attr("y", levelOffsets[i] - levelHeights[i])
                .attr("width", levelWidth)
                .attr("height", levelHeights[i])
                .attr("opacity", 0)
                .attr("pointer-events", "all")
                .on("mousemove", function(i) {
                    i = Number(i);
                    if (version.levels[i].length > 0) {
                        // The mouse coordinates are relative to the
                        // SVG element. Adjust to be relative to the
                        // level position.
                        var mousex = d3.mouse(this)[0] - lineStart;
                        var index = Math.round(mousex * version.scale(i));
                        if (index < 0) {
                            index = 0;
                        } else if (index >= version.levels[i].length) {
                            index = version.levels[i].length - 1;
                        }
                        var fileNum = version.levels[i][index];
                        var meta = data.Files[fileNum];

                        // Find the start and end index of the tables
                        // that overlap with filenum.
                        var overlapInfo = "";
                        for (var j = 1; j < version.levels.length; j++) {
                            indicators[j]
                                .attr("fill", "black")
                                .attr("opacity", 0.3)
                                .attr("y", levelOffsets[j] - levelHeights[j])
                                .attr("height", levelHeights[j]);
                            if (j == i) {
                                continue;
                            }
                            var fileNums = version.levels[j];
                            for (var k in fileNums) {
                                var other = data.Files[fileNums[k]];
                                if (other.Largest < meta.Smallest) {
                                    continue;
                                }
                                var s = version.scale(j);
                                var t = k;
                                for (; k < fileNums.length; k++) {
                                    var other = data.Files[fileNums[k]];
                                    if (other.Smallest >= meta.Largest) {
                                        break;
                                    }
                                }
                                if (k == t) {
                                    indicators[j]
                                        .attr("x", lineStart + s * t)
                                        .attr("width", s);
                                } else {
                                    indicators[j]
                                        .attr("x", lineStart + s * t)
                                        .attr(
                                            "width",
                                            Math.max(0.5, s * (k - t))
                                        );
                                }
                                if (i + 1 == j && k > t) {
                                    var overlapSize = version.levels[j]
                                        .slice(t, k)
                                        .reduce(
                                            (sum, elem) =>
                                                sum + data.Files[elem].Size,
                                            0
                                        );

                                    overlapInfo =
                                        " overlaps " +
                                        (k - t) +
                                        " @ L" +
                                        j +
                                        " (" +
                                        humanize(overlapSize) +
                                        ")";
                                }
                                break;
                            }
                        }

                        // TODO(peter): display smallest/largest key.
                        vis
                            .select("text.reason")
                            .text(
                                "[L" +
                                    i +
                                    " " +
                                    fileNum +
                                    " (" +
                                    humanize(data.Files[fileNum].Size) +
                                    ")" +
                                    overlapInfo +
                                    "]"
                            );

                        indicators[i]
                            .attr("x", lineStart + version.scale(i) * index)
                            .attr("width", 1);
                    }
                })
                .on("mouseout", function() {
                    vis.select("text.reason").text(d => info);
                    for (var i in indicators) {
                        indicators[i].attr("fill", "none");
                    }
                });

            // Scale each level to fit within the display.
            var s = this.scale(i);
            g.attr(
                "transform",
                "translate(" +
                    lineStart +
                    "," +
                    levelOffsets[i] +
                    ") scale(" +
                    s +
                    "," +
                    -(1 / s) +
                    ")"
            );

            // Render the sstables for the level.
            var level = g.selectAll("rect.L" + i).data(this.levels[i], d => d);
            level.attr("fill", "#555").attr("x", (fileNum, i) => i);
            level
                .enter()
                .append("rect")
                .attr("class", "L" + i)
                .attr("id", fileNum => fileNum)
                .attr("fill", "red")
                .attr("x", (fileNum, i) => i)
                .attr("y", 0)
                .attr("width", 1)
                .attr("height", fileNum => version.height(fileNum));
            level.exit().remove();
        }

        sliderHandle.attr("cx", sliderX(version.index));
        index.node().value = version.index;
    }
};

// Recalculate structures related to the page width.
function updateSize() {
    var svg = d3.select("#slider").html("");

    var margin = { right: 10, left: 10 };

    var width = styleWidth(d3.select("#slider")) - margin.left - margin.right,
        height = styleHeight(svg);

    sliderX = d3
        .scaleLinear()
        .domain([0, data.Edits.length - 1])
        .range([0, width])
        .clamp(true);

    var slider = svg
        .append("g")
        .attr("class", "slider")
        .attr("transform", "translate(" + margin.left + "," + height / 2 + ")");

    slider
        .append("line")
        .attr("class", "track")
        .attr("x1", sliderX.range()[0])
        .attr("x2", sliderX.range()[1])
        .select(function() {
            return this.parentNode.appendChild(this.cloneNode(true));
        })
        .attr("class", "track-inset")
        .select(function() {
            return this.parentNode.appendChild(this.cloneNode(true));
        })
        .attr("class", "track-overlay")
        .call(
            d3
                .drag()
                .on("start.interrupt", function() {
                    slider.interrupt();
                })
                .on("start drag", function() {
                    version.set(Math.round(sliderX.invert(d3.event.x)));
                })
        );

    slider
        .insert("g", ".track-overlay")
        .attr("class", "ticks")
        .attr("transform", "translate(0," + 18 + ")")
        .selectAll("text")
        .data(sliderX.ticks(10))
        .enter()
        .append("text")
        .attr("x", sliderX)
        .attr("text-anchor", "middle")
        .text(function(d) {
            return d;
        });

    sliderHandle = slider
        .insert("circle", ".track-overlay")
        .attr("class", "handle")
        .attr("r", 9)
        .attr("cx", sliderX(version.index));

    levelWidth = styleWidth(vis) - 10 - lineStart;
    var lineEnd = lineStart + levelWidth;

    vis
        .selectAll("line")
        .data(levelOffsets)
        .attr("x2", lineEnd)
        .enter()
        .append("line")
        .attr("x1", lineStart)
        .attr("x2", lineEnd)
        .attr("y1", d => d)
        .attr("y2", d => d)
        .attr("stroke", "#ddd");

    vis
        .selectAll("defs clipPath rect")
        .data(version.levels)
        .attr("width", lineEnd - lineStart)
        .enter()
        .append("defs")
        .append("clipPath")
        .attr("id", (d, i) => "L" + i)
        .append("rect")
        .attr("x", lineStart)
        .attr("y", (d, i) => levelOffsets[i] - levelHeights[i])
        .attr("width", lineEnd - lineStart)
        .attr("height", (d, i) => levelHeights[i]);
}

window.onload = function() {
    updateSize();
    version.set(0);
};

window.addEventListener("resize", function() {
    updateSize();
    version.render();
});

var timer;

function startPlayback(increment) {
    timer = d3.timer(function() {
        var lastIndex = version.index;
        version.set(version.index + increment);
        if (lastIndex == version.index) {
            timer.stop();
            timer = null;
        }
    });
}

function stopPlayback() {
    if (timer == null) {
        return false;
    }
    timer.stop();
    timer = null;
    return true;
}

document.addEventListener("keydown", function(e) {
    switch (e.keyCode) {
        case 37: // left arrow
            stopPlayback();
            version.set(version.index - (e.shiftKey ? 10 : 1));
            return;
        case 39: // right arrow
            stopPlayback();
            version.set(version.index + (e.shiftKey ? 10 : 1));
            return;
        case 32: // space
            if (stopPlayback()) {
                return;
            }
            startPlayback(1);
            return;
    }
});

index.on("input", function() {
    if (!isNaN(+this.value)) {
        version.set(Number(this.value));
    }
});
