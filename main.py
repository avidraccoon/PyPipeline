from pipeline_base import Pipeline, stage, provider



pipeline = (
    Pipeline()
    .if_("debug_mode", lambda b: (
        b.match("mode", lambda m: (
            m.case("verbose").stage(display_debug_verbose),
            m.case("compact").stage(display_debug_compact),
        ))
    ))
    .elif_("debug_mode", lambda b: (
        b.stage(display_debug_alt)
    ))
    .else_(lambda b: (
        b.stage(display_normal),
    ))
)



pipeline.run_sync()