extern crate proc_macro;
use proc_macro::TokenStream;

#[proc_macro]
pub fn repeat(args: TokenStream) -> TokenStream {
    let args = args.to_string();
    let (num, what) = args.split_once(",").unwrap();
    let num: usize = num.parse().unwrap();

    let (base, what) = what.split_once(",").unwrap();
    let mut res = base.to_string();
    for _ in 0..num {
        res += ".";
        res += what;
    }
    res.parse().unwrap()
}
