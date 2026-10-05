use std::hint::black_box;

use muxe_core::{
    CompiledGeneration, ConditionProgram, ConfigDocument, KeyCapabilities, PagesContext, SourceId,
    SourceSpan, compile_yaml,
};

const COMPLETE_BASE: &str = include_str!("../tests/fixtures/representative.yml");

const PAGER_CONDITION: &str = "pages.current < pages.count && pages.count > 1";

fn main() {
    divan::main();
}

// Input preparation and output destruction are outside the measured operation.
// Return the owned result to the harness instead of dropping it in the closure.
#[divan::bench]
fn parse_config_document(bencher: divan::Bencher<'_, '_>) {
    bencher
        .with_inputs(|| SourceId::new("complete-base.yml"))
        .bench_values(|source| {
            ConfigDocument::parse(black_box(source), black_box(COMPLETE_BASE))
                .expect("representative config parses")
        });
}

#[divan::bench]
fn compile_config_yaml(bencher: divan::Bencher<'_, '_>) {
    bencher
        .with_inputs(|| {
            (
                SourceId::new("complete-base.yml"),
                KeyCapabilities::default(),
            )
        })
        .bench_values(|(source, key_capabilities)| {
            compile_yaml(
                CompiledGeneration(1),
                black_box(source),
                black_box(COMPLETE_BASE),
                black_box(key_capabilities),
                None,
            )
            .expect("representative config compiles")
        });
}

#[divan::bench]
fn compile_condition(bencher: divan::Bencher<'_, '_>) {
    bencher
        .with_inputs(|| SourceSpan::new(SourceId::new("condition"), 0, PAGER_CONDITION.len()))
        .bench_values(|span| {
            ConditionProgram::compile(black_box(PAGER_CONDITION), black_box(span))
                .expect("pager condition compiles")
        });
}

#[divan::bench]
fn evaluate_condition(bencher: divan::Bencher<'_, '_>) {
    let span = SourceSpan::new(SourceId::new("condition"), 0, PAGER_CONDITION.len());
    let program =
        ConditionProgram::compile(PAGER_CONDITION, span).expect("pager condition compiles");
    bencher.bench(|| {
        program
            .evaluate(black_box(PagesContext {
                count: 4,
                current: 2,
            }))
            .expect("pager condition evaluates")
    });
}
