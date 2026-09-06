use std::hint::black_box;

use muxe_terminal_input::Parser;

const MIXED_INPUT: &[u8] = b"a\xC3\xA5\x1b[97:65:113;198:2u\x1b[?7u\x1b[1;3A\x1bOP\r";
const FRAGMENTED_KITTY: [&[u8]; 3] = [b"\x1b[97:65", b":113;198:2", b"u\x1b[?7u"];

fn main() {
    divan::main();
}

#[divan::bench]
fn mixed_vt100_and_kitty_stream(bencher: divan::Bencher<'_, '_>) {
    bencher.bench(|| {
        let mut parser = Parser::new();
        parser.push(black_box(MIXED_INPUT), |event| {
            black_box(event);
        });
        parser.finish(|event| {
            black_box(event);
        });
    });
}

#[divan::bench]
fn fragmented_kitty_stream(bencher: divan::Bencher<'_, '_>) {
    bencher.bench(|| {
        let mut parser = Parser::new();
        for chunk in black_box(FRAGMENTED_KITTY) {
            parser.push(chunk, |event| {
                black_box(event);
            });
        }
        parser.finish(|event| {
            black_box(event);
        });
    });
}
