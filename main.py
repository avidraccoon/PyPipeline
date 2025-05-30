from pipeline_base import Pipeline, stage, provider


pipeline = (
    Pipeline()
    .if_("debug_mode", lambda b: (
        b.match("mode", lambda m: (
            m.case("verbose")
                .stage(display_debug_verbose),
            m.case("compact")
                .stage(display_debug_compact),
        ))
    ))
    .else_(lambda b: (
        b.stage(display_normal),
    ))
)


pipeline = (
    Pipeline()
        .match("mode", lambda m: (
            m.case("debug_verbose")
                .stage(display_debug_verbose),
            m.case("debug_compact")
                .stage(display_debug_compact),
            m.default()
                .stage(display_normal)
        ))
)


=>

pipeline = (
    Pipeline()
        .match(True, lambda b: (
            b.case("debug_mode")
                .match("mode", lambda m: (
                    m.case("verbose")
                        .stage(display_debug_verbose),
                    m.case("compact")
                        .stage(display_debug_compact),
                )),
            b.default()
                .stage(display_debug_compact)
        ))
)


pipeline.run_sync()