//! One seam type for test-time behavior substitution.
//!
//! A [`Seam`] is a named place in a program whose behavior a test can choose
//! from outside, without editing the place (Feathers, *Working Effectively
//! with Legacy Code*). Its enabling point is a slot: empty in production, so
//! [`Seam::with`] returns `None` and the caller runs its default; filled by a
//! test through [`Seam::install`], which returns a guard that empties the slot
//! again on drop.
//!
//! The type knows nothing about what the installed behavior does. That is the
//! behavior trait `B`: a [`Decide`] at a named site answers fire or pass, a
//! clock returns a time, a decorator wraps a dependency. Consuming crates
//! declare their seams as statics of this type and keep the traits that need
//! their own types (errors, clocks, adapters) beside them.
//!
//! Two slot scopes exist because the seams a program has do not share one:
//! [`Global`] for a site that any thread may cross, [`ThreadLocal`] for a
//! per-thread stream such as a seeded id source. A consuming crate picks the
//! scope per seam when it declares the static.
//!
//! Filling a slot needs the `install` feature; without it the crate is
//! read-only, so arming a seam in a build that never reads one is a compile
//! error instead of a silent no-op.

use std::cell::RefCell;
use std::fmt;
use std::marker::PhantomData;
use std::panic::Location;
use std::sync::atomic::{AtomicBool, AtomicPtr, AtomicU64, Ordering};
use std::sync::{Arc, Condvar, Mutex, RwLock};
use std::thread::LocalKey;

/// Everything a slot may hold. The one hook every behavior shares is
/// [`Behavior::uninstalling`], called by the guard's drop before the slot is
/// emptied, so a behavior that parks a caller can release it.
pub trait Behavior: 'static {
    fn uninstalling(&self) {}
}

/// Storage behind a seam's enabling point: one cell holding the installed
/// behavior, or nothing.
pub trait Storage<B: ?Sized + 'static>: 'static {
    /// Marker carried by the guard: `()` for a scope whose guard may move
    /// between threads, a raw-pointer marker for one whose guard must drop on
    /// the installing thread.
    type Token: Default;
    fn get(&self) -> Option<Arc<B>>;
    fn replace(&self, value: Option<Arc<B>>) -> Option<Arc<B>>;
    /// Fill an empty slot, or hand `value` back when the slot is occupied;
    /// one critical section, so two fillers cannot both succeed.
    fn fill(&self, value: Arc<B>) -> Result<(), Arc<B>>;
    /// Empty the slot only if it still holds `own` (by address); one critical
    /// section, so a guard never removes a later installation.
    fn take_if(&self, own: &Arc<B>) -> Option<Arc<B>>;
}

fn same_allocation<B: ?Sized>(a: &Arc<B>, b: &Arc<B>) -> bool {
    std::ptr::addr_eq(Arc::as_ptr(a), Arc::as_ptr(b))
}

/// Process-wide slot: one lock, readable from any thread. A `static` of a
/// seam with this scope needs `B: Send + Sync`.
pub struct Global<B: ?Sized + 'static>(RwLock<Option<Arc<B>>>);

impl<B: ?Sized + 'static> Global<B> {
    pub const fn new() -> Self {
        Self(RwLock::new(None))
    }
}

impl<B: ?Sized + 'static> Default for Global<B> {
    fn default() -> Self {
        Self::new()
    }
}

impl<B: ?Sized + Send + Sync + 'static> Storage<B> for Global<B> {
    type Token = ();

    fn get(&self) -> Option<Arc<B>> {
        self.0
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone()
    }

    fn replace(&self, value: Option<Arc<B>>) -> Option<Arc<B>> {
        std::mem::replace(
            &mut *self
                .0
                .write()
                .unwrap_or_else(std::sync::PoisonError::into_inner),
            value,
        )
    }

    fn fill(&self, value: Arc<B>) -> Result<(), Arc<B>> {
        let mut slot = self
            .0
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if slot.is_some() {
            return Err(value);
        }
        *slot = Some(value);
        Ok(())
    }

    fn take_if(&self, own: &Arc<B>) -> Option<Arc<B>> {
        let mut slot = self
            .0
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        match &*slot {
            Some(current) if same_allocation(current, own) => slot.take(),
            _ => None,
        }
    }
}

/// Per-thread slot over a `thread_local!` cell declared by the consuming
/// crate through [`thread_local_seam!`]. Installing on one thread leaves every
/// other thread's slot empty, and the guard must drop on the installing thread.
pub struct ThreadLocal<B: ?Sized + 'static>(&'static LocalKey<RefCell<Option<Arc<B>>>>);

impl<B: ?Sized + 'static> ThreadLocal<B> {
    pub const fn new(key: &'static LocalKey<RefCell<Option<Arc<B>>>>) -> Self {
        Self(key)
    }
}

/// Guard marker that is `!Send`, so a thread-local guard cannot leave the
/// thread whose slot it fills.
#[derive(Default)]
pub struct ThreadBound(PhantomData<*const ()>);

impl<B: ?Sized + 'static> Storage<B> for ThreadLocal<B> {
    type Token = ThreadBound;

    fn get(&self) -> Option<Arc<B>> {
        self.0.with(|cell| cell.borrow().clone())
    }

    fn replace(&self, value: Option<Arc<B>>) -> Option<Arc<B>> {
        self.0
            .with(|cell| std::mem::replace(&mut *cell.borrow_mut(), value))
    }

    fn fill(&self, value: Arc<B>) -> Result<(), Arc<B>> {
        self.0.with(|cell| {
            let mut slot = cell.borrow_mut();
            if slot.is_some() {
                return Err(value);
            }
            *slot = Some(value);
            Ok(())
        })
    }

    fn take_if(&self, own: &Arc<B>) -> Option<Arc<B>> {
        self.0.with(|cell| {
            let mut slot = cell.borrow_mut();
            match &*slot {
                Some(current) if same_allocation(current, own) => slot.take(),
                _ => None,
            }
        })
    }
}

/// A named site with a slot. `name` is the identity a test and a catalog use;
/// `op` says which operation crosses it; `effects` is non-empty only for
/// [`Decide`] seams and lists every outcome the site honors when the
/// decision is [`Decision::Fire`]; `site` is where the static is declared,
/// recorded by the compiler, which is why a seam is declared beside the code
/// it guards and only indexed by the catalog.
pub struct Seam<B: ?Sized + 'static, S: Storage<B> = Global<B>> {
    name: &'static str,
    op: Op,
    effects: &'static [Effect],
    site: &'static Location<'static>,
    last_fired: AtomicPtr<Location<'static>>,
    slot: S,
    _behavior: PhantomData<fn() -> Arc<B>>,
}

impl<B: ?Sized + 'static, S: Storage<B>> Seam<B, S> {
    /// A seam that is not a decision site (a clock, an id source, a
    /// decorator).
    #[track_caller]
    pub const fn new(name: &'static str, op: Op, slot: S) -> Self {
        Self {
            name,
            op,
            effects: &[],
            site: Location::caller(),
            last_fired: AtomicPtr::new(std::ptr::null_mut()),
            slot,
            _behavior: PhantomData,
        }
    }

    /// A decision site: the installed [`Decide`] answers pass or fire with
    /// one of `effects`, and the site acts per that effect.
    ///
    /// # Panics
    ///
    /// When `effects` is empty: a decision site with nothing to fire is a
    /// non-decision seam, declared with [`Seam::new`].
    #[track_caller]
    pub const fn decide(name: &'static str, op: Op, effects: &'static [Effect], slot: S) -> Self {
        assert!(
            !effects.is_empty(),
            "a decision seam declares at least one effect"
        );
        Self {
            name,
            op,
            effects,
            site: Location::caller(),
            last_fired: AtomicPtr::new(std::ptr::null_mut()),
            slot,
            _behavior: PhantomData,
        }
    }

    pub const fn name(&self) -> &'static str {
        self.name
    }

    pub const fn op(&self) -> Op {
        self.op
    }

    /// The outcomes this site honors; empty for a non-decision seam.
    pub const fn effects(&self) -> &'static [Effect] {
        self.effects
    }

    /// Where the static is declared: the file and line the compiler saw.
    pub const fn site(&self) -> &'static Location<'static> {
        self.site
    }

    /// Where the decision last fired, once it has: the helper call whose
    /// crossing fired, one of possibly several for one seam. Passing
    /// crossings do not move it.
    pub fn last_fired(&self) -> Option<&'static Location<'static>> {
        let raw = self.last_fired.load(Ordering::Relaxed);
        // SAFETY: the pointer is null or was stored from a `&'static Location`.
        unsafe { raw.as_ref() }
    }

    /// Hand the installed behavior to `f`, or `None` when the slot is empty.
    /// The slot's lock or borrow is released before `f` runs, so `f` may
    /// re-enter the same seam.
    pub fn with<R>(&self, f: impl FnOnce(&B) -> R) -> Option<R> {
        let installed = self.slot.get()?;
        Some(f(&installed))
    }
}

#[cfg(any(test, feature = "install"))]
impl<B: ?Sized + Behavior, S: Storage<B>> Seam<B, S> {
    /// Fill the slot; the returned guard empties it on drop, after calling
    /// [`Behavior::uninstalling`] on the value it removes.
    ///
    /// # Panics
    ///
    /// When the slot is already filled: two live installs on one seam would
    /// let the earlier guard's drop empty the later one's slot.
    pub fn install(&'static self, behavior: Arc<B>) -> Installed<B, S> {
        let own = Arc::clone(&behavior);
        assert!(
            self.slot.fill(behavior).is_ok(),
            "seam {} is already installed; drop the earlier guard first",
            self.name
        );
        Installed {
            seam: self,
            own,
            _token: S::Token::default(),
        }
    }

    /// Empty the slot outside a guard (a scenario teardown).
    pub fn clear(&self) {
        if let Some(previous) = self.slot.replace(None) {
            previous.uninstalling();
        }
    }
}

impl<B: ?Sized + 'static, S: Storage<B>> fmt::Debug for Seam<B, S> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Seam")
            .field("name", &self.name)
            .field("op", &self.op)
            .field("effects", &self.effects)
            .field("site", &self.site)
            .finish()
    }
}

/// The guard [`Seam::install`] returns. Dropping it empties the slot when the
/// slot still holds this guard's own installation; a value installed after a
/// [`Seam::clear`] belongs to its own guard and is left in place.
#[cfg(any(test, feature = "install"))]
#[must_use = "dropping the guard uninstalls the behavior at once"]
pub struct Installed<B: ?Sized + Behavior, S: Storage<B>> {
    seam: &'static Seam<B, S>,
    own: Arc<B>,
    _token: S::Token,
}

#[cfg(any(test, feature = "install"))]
impl<B: ?Sized + Behavior, S: Storage<B>> Drop for Installed<B, S> {
    fn drop(&mut self) {
        if let Some(previous) = self.seam.slot.take_if(&self.own) {
            previous.uninstalling();
        }
    }
}

/// Which GQT step kind crosses a seam. `AnyWrite` names a sub-operation every
/// write step may reach; `Unreachable` names an operation no GQ statement
/// starts (reads included, until a read step kind exists), a debt the
/// coverage listing reports.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum Op {
    Mutation,
    BranchMerge,
    BranchCreate,
    BranchDelete,
    AnyWrite,
    Unreachable,
}

impl Op {
    pub const fn as_str(self) -> &'static str {
        match self {
            Op::Mutation => "mutation",
            Op::BranchMerge => "branch_merge",
            Op::BranchCreate => "branch_create",
            Op::BranchDelete => "branch_delete",
            Op::AnyWrite => "any_write",
            Op::Unreachable => "unreachable",
        }
    }
}

/// An outcome a decision site honors when a decision fires. Parking a
/// caller is not an outcome: [`Hold`] is a decider that passes after the
/// park, installable on any decision seam.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum Effect {
    Fail,
    Skip,
    Contention,
}

impl Effect {
    pub const fn as_str(self) -> &'static str {
        match self {
            Effect::Fail => "fail",
            Effect::Skip => "skip",
            Effect::Contention => "contention",
        }
    }
}

/// The answer a [`Decide`] gives at a crossing: pass, or fire with the
/// outcome the site is to produce, one of the seam's declared effects.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Decision {
    Fire(Effect),
    Pass,
}

/// The behavior of a decision site: called once per crossing, may block, may
/// keep state (a counter, a script). `name` is the crossed seam's name.
pub trait Decide: Behavior + Send + Sync {
    fn decide(&self, name: &'static str) -> Decision;
}

/// A decision seam with the process-wide scope, the shape every named site
/// in a consuming crate's catalog has.
pub type DecideSeam = Seam<dyn Decide, Global<dyn Decide>>;

impl DecideSeam {
    /// Ask the installed decider from the caller's location; an empty slot
    /// is `Pass`. A helper marked `#[track_caller]` forwards its own caller,
    /// so the recorded crossing is the site, not the helper.
    ///
    /// # Panics
    ///
    /// When the decider fires an effect the seam does not declare: the site
    /// has no arm for it, so the installer, not the site, is wrong.
    #[track_caller]
    pub fn crossed(&self) -> Decision {
        self.crossed_from(Location::caller())
    }

    /// [`crossed`](Self::crossed) with the crossing location passed in, for
    /// a helper that captured its caller before an `async` block.
    pub fn crossed_from(&self, caller: &'static Location<'static>) -> Decision {
        let decision = self.with(|d| d.decide(self.name)).unwrap_or(Decision::Pass);
        if let Decision::Fire(effect) = decision {
            assert!(
                self.effects.contains(&effect),
                "seam {} fired {} but declares only {}",
                self.name,
                effect.as_str(),
                effects_list(self.effects)
            );
            self.last_fired
                .store(std::ptr::from_ref(caller).cast_mut(), Ordering::Relaxed);
        }
        decision
    }
}

/// The spelling of an effect set in a message: `[fail, skip]`.
pub fn effects_list(effects: &[Effect]) -> String {
    let names: Vec<&str> = effects.iter().map(|e| e.as_str()).collect();
    format!("[{}]", names.join(", "))
}

#[cfg(any(test, feature = "decide"))]
impl DecideSeam {
    /// The effect the plain installers fire; panics on a multi-effect seam.
    fn sole_effect(&self) -> Effect {
        match self.effects {
            [effect] => *effect,
            effects => panic!(
                "seam {} declares {}; install with fire_always_with, fire_once_at_with or count_and_fire_at_with and name the effect",
                self.name,
                effects_list(effects)
            ),
        }
    }

    fn declared(&self, effect: Effect) -> Effect {
        assert!(
            self.effects.contains(&effect),
            "seam {} does not declare {}; it declares {}",
            self.name,
            effect.as_str(),
            effects_list(self.effects)
        );
        effect
    }

    /// Fire the seam's one effect on every crossing until the guard drops.
    pub fn fire_always(&'static self) -> Installed<dyn Decide, Global<dyn Decide>> {
        self.fire_always_with(self.sole_effect())
    }

    /// Fire `effect`, one of the declared effects, on every crossing.
    pub fn fire_always_with(
        &'static self,
        effect: Effect,
    ) -> Installed<dyn Decide, Global<dyn Decide>> {
        self.install(Arc::new(FireAlways(self.declared(effect))))
    }

    /// Pass on the first `n - 1` crossings, fire the seam's one effect on
    /// the `n`th, pass after. `n` is at least 1.
    pub fn fire_once_at(&'static self, n: u64) -> Installed<dyn Decide, Global<dyn Decide>> {
        self.fire_once_at_with(n, self.sole_effect())
    }

    /// [`fire_once_at`](Self::fire_once_at) firing `effect`, one of the
    /// declared effects.
    pub fn fire_once_at_with(
        &'static self,
        n: u64,
        effect: Effect,
    ) -> Installed<dyn Decide, Global<dyn Decide>> {
        self.install(Arc::new(FireOnceAt::new(n, self.declared(effect))))
    }

    /// Panic at every crossing, a simulated process death; the message text
    /// is the one crash tests match.
    pub fn panic_at(&'static self) -> Installed<dyn Decide, Global<dyn Decide>> {
        self.install(Arc::new(PanicAt))
    }

    /// Park the first crossing until [`Hold::release`] or the guard drops;
    /// later crossings pass. Returns the guard and the handle the test releases
    /// and observes through.
    pub fn hold(&'static self) -> (Installed<dyn Decide, Global<dyn Decide>>, Arc<Hold>) {
        let hold = Arc::new(Hold::default());
        (self.install(hold.clone()), hold)
    }

    /// Run `f` at every crossing and pass, for probes and counters.
    pub fn observe<F>(&'static self, f: F) -> Installed<dyn Decide, Global<dyn Decide>>
    where
        F: Fn() + Send + Sync + 'static,
    {
        self.install(Arc::new(Observe(f)))
    }

    /// [`fire_once_at_with`](Self::fire_once_at_with) spelled by effect.
    pub fn fail_once_at(&'static self, n: u64) -> Installed<dyn Decide, Global<dyn Decide>> {
        self.fire_once_at_with(n, Effect::Fail)
    }

    pub fn skip_once_at(&'static self, n: u64) -> Installed<dyn Decide, Global<dyn Decide>> {
        self.fire_once_at_with(n, Effect::Skip)
    }

    pub fn contention_once_at(&'static self, n: u64) -> Installed<dyn Decide, Global<dyn Decide>> {
        self.fire_once_at_with(n, Effect::Contention)
    }

    /// [`fire_always_with`](Self::fire_always_with) spelled by effect.
    pub fn fail_always(&'static self) -> Installed<dyn Decide, Global<dyn Decide>> {
        self.fire_always_with(Effect::Fail)
    }

    pub fn skip_always(&'static self) -> Installed<dyn Decide, Global<dyn Decide>> {
        self.fire_always_with(Effect::Skip)
    }

    pub fn contention_always(&'static self) -> Installed<dyn Decide, Global<dyn Decide>> {
        self.fire_always_with(Effect::Contention)
    }

    /// Count crossings and fire the seam's one effect on `n`; the count is
    /// readable by the test, which is how a runner proves delivery without
    /// an error message.
    pub fn count_and_fire_at(
        &'static self,
        n: u64,
    ) -> (Installed<dyn Decide, Global<dyn Decide>>, Arc<Counted>) {
        self.count_and_fire_at_with(n, self.sole_effect())
    }

    /// [`count_and_fire_at`](Self::count_and_fire_at) firing `effect`, one
    /// of the declared effects.
    pub fn count_and_fire_at_with(
        &'static self,
        n: u64,
        effect: Effect,
    ) -> (Installed<dyn Decide, Global<dyn Decide>>, Arc<Counted>) {
        let counted = Arc::new(Counted::new(n, self.declared(effect)));
        (self.install(counted.clone()), counted)
    }
}

/// Fires its effect on every crossing.
pub struct FireAlways(pub Effect);

impl Behavior for FireAlways {}

impl Decide for FireAlways {
    fn decide(&self, _name: &'static str) -> Decision {
        Decision::Fire(self.0)
    }
}

/// Fires its effect exactly once, on the `n`th crossing.
pub struct FireOnceAt {
    target: u64,
    effect: Effect,
    crossings: AtomicU64,
}

impl FireOnceAt {
    /// # Panics
    ///
    /// When `n` is 0: crossings are counted from 1.
    pub fn new(n: u64, effect: Effect) -> Self {
        assert!(n >= 1, "fire_once_at counts crossings from 1, got 0");
        Self {
            target: n,
            effect,
            crossings: AtomicU64::new(0),
        }
    }
}

impl Behavior for FireOnceAt {}

impl Decide for FireOnceAt {
    fn decide(&self, _name: &'static str) -> Decision {
        let seen = self.crossings.fetch_add(1, Ordering::SeqCst) + 1;
        if seen == self.target {
            Decision::Fire(self.effect)
        } else {
            Decision::Pass
        }
    }
}

/// Counts every crossing and fires its effect on the `n`th; the count is
/// the delivery record a runner reads after the step.
pub struct Counted {
    inner: FireOnceAt,
    fired: AtomicBool,
}

impl Counted {
    pub fn new(n: u64, effect: Effect) -> Self {
        Self {
            inner: FireOnceAt::new(n, effect),
            fired: AtomicBool::new(false),
        }
    }

    pub fn crossings(&self) -> u64 {
        self.inner.crossings.load(Ordering::SeqCst)
    }

    pub fn fired(&self) -> bool {
        self.fired.load(Ordering::SeqCst)
    }

    /// The effect this decider fires.
    pub fn effect(&self) -> Effect {
        self.inner.effect
    }
}

impl Behavior for Counted {}

impl Decide for Counted {
    fn decide(&self, name: &'static str) -> Decision {
        let decision = self.inner.decide(name);
        if decision != Decision::Pass {
            self.fired.store(true, Ordering::SeqCst);
        }
        decision
    }
}

/// Panics at the first crossing with the `fail` crate's message.
pub struct PanicAt;

impl Behavior for PanicAt {}

impl Decide for PanicAt {
    fn decide(&self, name: &'static str) -> Decision {
        panic!("failpoint {name} panic")
    }
}

/// Parks the first crossing until released; later crossings pass. Dropping
/// the guard releases a parked caller through [`Behavior::uninstalling`].
#[derive(Default)]
pub struct Hold {
    state: Mutex<HoldState>,
    changed: Condvar,
}

#[derive(Default)]
struct HoldState {
    reached: bool,
    released: bool,
    timed_out: bool,
}

impl Hold {
    /// Let the parked caller continue.
    pub fn release(&self) {
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        state.released = true;
        self.changed.notify_all();
    }

    /// Whether any caller has reached the seam.
    pub fn reached(&self) -> bool {
        self.state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .reached
    }

    /// Whether the parked caller resumed at [`HOLD_BOUND`] instead of a
    /// release: a test that expected to release it has lost its ordering.
    pub fn timed_out(&self) -> bool {
        self.state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .timed_out
    }

    /// Block until a caller reaches the seam, at most [`HOLD_BOUND`].
    ///
    /// # Panics
    ///
    /// When no caller reaches the seam within the bound.
    pub fn wait_until_reached(&self) {
        let deadline = std::time::Instant::now() + HOLD_BOUND;
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        while !state.reached {
            let remaining = deadline.saturating_duration_since(std::time::Instant::now());
            assert!(
                !remaining.is_zero(),
                "hold: no caller reached the seam within {HOLD_BOUND:?}"
            );
            state = self
                .changed
                .wait_timeout(state, remaining)
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .0;
        }
    }
}

impl Behavior for Hold {
    fn uninstalling(&self) {
        self.release();
    }
}

impl Decide for Hold {
    fn decide(&self, _name: &'static str) -> Decision {
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if state.reached {
            return Decision::Pass;
        }
        state.reached = true;
        self.changed.notify_all();
        let deadline = std::time::Instant::now() + HOLD_BOUND;
        while !state.released {
            let remaining = deadline.saturating_duration_since(std::time::Instant::now());
            if remaining.is_zero() {
                state.timed_out = true;
                break;
            }
            state = self
                .changed
                .wait_timeout(state, remaining)
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .0;
        }
        Decision::Pass
    }
}

/// The longest a [`Hold`] parks a caller: a test that never releases (it is
/// awaiting the parked work on the same thread, say) resumes here instead of
/// wedging the binary, and [`Hold::timed_out`] records that it did.
pub const HOLD_BOUND: std::time::Duration = std::time::Duration::from_secs(30);

/// Runs a closure at every crossing and passes.
pub struct Observe<F>(pub F);

impl<F: Fn() + Send + Sync + 'static> Behavior for Observe<F> {}

impl<F: Fn() + Send + Sync + 'static> Decide for Observe<F> {
    fn decide(&self, _name: &'static str) -> Decision {
        (self.0)();
        Decision::Pass
    }
}

/// The erased row a catalog lists: what a case format and a generator need
/// to know about a seam without its behavior type.
pub trait SeamEntry: Sync {
    fn name(&self) -> &'static str;
    fn op(&self) -> Op;
    /// The outcomes the site honors; empty for a non-decision seam.
    fn effects(&self) -> &'static [Effect];
    /// Where the static is declared.
    fn site(&self) -> &'static Location<'static>;
    /// Where the decision last fired, once it has.
    fn last_fired(&self) -> Option<&'static Location<'static>>;
    /// The seam as a decision seam, when it is one.
    fn as_decide(&'static self) -> Option<&'static DecideSeam>;
    /// Empty the slot (a scenario teardown over a whole catalog).
    #[cfg(any(test, feature = "decide"))]
    fn clear(&self);
}

impl SeamEntry for DecideSeam {
    fn name(&self) -> &'static str {
        self.name
    }

    fn op(&self) -> Op {
        self.op
    }

    fn effects(&self) -> &'static [Effect] {
        self.effects
    }

    fn site(&self) -> &'static Location<'static> {
        self.site
    }

    fn last_fired(&self) -> Option<&'static Location<'static>> {
        Seam::last_fired(self)
    }

    fn as_decide(&'static self) -> Option<&'static DecideSeam> {
        Some(self)
    }

    #[cfg(any(test, feature = "decide"))]
    fn clear(&self) {
        Seam::clear(self)
    }
}

/// Declare a decision seam beside the site it guards:
/// `decide_seam! { pub static NAME = ("area.place", Mutation, [Fail, Skip]); }`.
/// The compiler records the invocation as the seam's site.
#[macro_export]
macro_rules! decide_seam {
    ($(#[$meta:meta])* $vis:vis static $name:ident = ($seam_name:literal, $op:ident, [$($effect:ident),+ $(,)?]);) => {
        $(#[$meta])*
        $vis static $name: $crate::DecideSeam = $crate::Seam::decide(
            $seam_name,
            $crate::Op::$op,
            &[$($crate::Effect::$effect),+],
            $crate::Global::new(),
        );
    };
}

/// Index a crate's decision seams from one list: re-export each under the
/// catalog module and list them in `ALL`.
#[macro_export]
macro_rules! catalog {
    ($($seam:path),+ $(,)?) => {
        $(pub use $seam;)+
        /// Every decision seam in this crate.
        pub static ALL: &[&'static dyn $crate::SeamEntry] = &[$(&$seam),+];
    };
}

/// Declare a thread-local seam, `thread_local_seam! { pub static CLOCK: dyn Clock =
/// ("clock", Op::Unreachable); }`; the cell is scoped inside the static's
/// initializer, so one module may declare several.
#[macro_export]
macro_rules! thread_local_seam {
    ($(#[$meta:meta])* $vis:vis static $name:ident : $behavior:ty = ($seam_name:expr, $op:expr);) => {
        $(#[$meta])*
        $vis static $name: $crate::Seam<$behavior, $crate::ThreadLocal<$behavior>> = {
            ::std::thread_local! {
                static SLOT: ::std::cell::RefCell<::std::option::Option<::std::sync::Arc<$behavior>>> =
                    const { ::std::cell::RefCell::new(::std::option::Option::None) };
            }
            $crate::Seam::new($seam_name, $op, $crate::ThreadLocal::new(&SLOT))
        };
    };
}

#[cfg(test)]
mod tests {
    use super::*;

    static SITE: DecideSeam =
        Seam::decide("test.site", Op::Mutation, &[Effect::Fail], Global::new());
    static COUNTED: DecideSeam =
        Seam::decide("test.counted", Op::Mutation, &[Effect::Fail], Global::new());
    static HELD: DecideSeam =
        Seam::decide("test.held", Op::Mutation, &[Effect::Fail], Global::new());
    static HELD_DROP: DecideSeam = Seam::decide(
        "test.held_drop",
        Op::Mutation,
        &[Effect::Fail],
        Global::new(),
    );
    static ROW: DecideSeam = Seam::decide("test.row", Op::Mutation, &[Effect::Fail], Global::new());
    static PUT: DecideSeam = Seam::decide(
        "test.put",
        Op::Mutation,
        &[Effect::Fail, Effect::Skip],
        Global::new(),
    );
    static PUT_PLAIN: DecideSeam = Seam::decide(
        "test.put_plain",
        Op::Mutation,
        &[Effect::Fail, Effect::Skip],
        Global::new(),
    );
    static PUT_UNDECLARED: DecideSeam = Seam::decide(
        "test.put_undeclared",
        Op::Mutation,
        &[Effect::Fail, Effect::Skip],
        Global::new(),
    );
    static PUT_LIAR: DecideSeam = Seam::decide(
        "test.put_liar",
        Op::Mutation,
        &[Effect::Skip],
        Global::new(),
    );

    trait Clock: Behavior + Send + Sync {
        fn now(&self) -> u64;
    }

    struct Fixed(u64);
    impl Behavior for Fixed {}
    impl Clock for Fixed {
        fn now(&self) -> u64 {
            self.0
        }
    }

    thread_local_seam! {
        static CLOCK: dyn Clock = ("test.clock", Op::Unreachable);
    }
    thread_local_seam! {
        static SECOND_CLOCK: dyn Clock = ("test.second_clock", Op::Unreachable);
    }

    #[test]
    fn empty_slot_passes_and_guard_drop_empties() {
        assert_eq!(SITE.crossed(), Decision::Pass);
        {
            let _guard = SITE.fire_always();
            assert_eq!(SITE.crossed(), Decision::Fire(Effect::Fail));
            assert_eq!(SITE.crossed(), Decision::Fire(Effect::Fail));
        }
        assert_eq!(SITE.crossed(), Decision::Pass);
    }

    #[test]
    fn fire_once_at_counts_crossings() {
        let (_guard, counted) = COUNTED.count_and_fire_at(3);
        assert_eq!(COUNTED.crossed(), Decision::Pass);
        assert_eq!(COUNTED.crossed(), Decision::Pass);
        assert_eq!(COUNTED.crossed(), Decision::Fire(Effect::Fail));
        assert_eq!(COUNTED.crossed(), Decision::Pass);
        assert_eq!(counted.crossings(), 4);
        assert!(counted.fired());
        assert_eq!(counted.effect(), Effect::Fail);
    }

    #[test]
    fn a_multi_effect_seam_fires_the_effect_named_at_install() {
        {
            let _guard = PUT.skip_once_at(1);
            assert_eq!(PUT.crossed(), Decision::Fire(Effect::Skip));
            assert_eq!(PUT.crossed(), Decision::Pass);
        }
        {
            let _guard = PUT.fail_always();
            assert_eq!(PUT.crossed(), Decision::Fire(Effect::Fail));
        }
        let (_guard, counted) = PUT.count_and_fire_at_with(2, Effect::Fail);
        assert_eq!(PUT.crossed(), Decision::Pass);
        assert_eq!(PUT.crossed(), Decision::Fire(Effect::Fail));
        assert_eq!(counted.effect(), Effect::Fail);
    }

    #[test]
    #[should_panic(expected = "declares [fail, skip]; install with")]
    fn a_multi_effect_seam_refuses_the_plain_installer() {
        let _guard = PUT_PLAIN.fire_once_at(1);
    }

    #[test]
    #[should_panic(expected = "does not declare contention")]
    fn an_installer_refuses_an_undeclared_effect() {
        let _guard = PUT_UNDECLARED.fire_always_with(Effect::Contention);
    }

    #[test]
    #[should_panic(expected = "fired fail but declares only [skip]")]
    fn a_crossing_refuses_a_decider_firing_outside_the_set() {
        let _guard = PUT_LIAR.install(Arc::new(FireAlways(Effect::Fail)));
        let _ = PUT_LIAR.crossed();
    }

    #[test]
    fn hold_parks_first_caller_until_released() {
        let (guard, hold) = HELD.hold();
        let worker = std::thread::spawn(|| HELD.crossed());
        hold.wait_until_reached();
        assert!(!worker.is_finished());
        hold.release();
        assert_eq!(worker.join().unwrap(), Decision::Pass);
        assert_eq!(HELD.crossed(), Decision::Pass);
        drop(guard);
    }

    #[test]
    fn dropping_the_guard_releases_a_parked_caller() {
        let (guard, hold) = HELD_DROP.hold();
        let worker = std::thread::spawn(|| HELD_DROP.crossed());
        hold.wait_until_reached();
        drop(guard);
        assert_eq!(worker.join().unwrap(), Decision::Pass);
    }

    #[test]
    fn thread_local_slot_is_per_thread() {
        let _guard = CLOCK.install(Arc::new(Fixed(7)));
        let _second = SECOND_CLOCK.install(Arc::new(Fixed(9)));
        assert_eq!(CLOCK.with(|c| c.now()), Some(7));
        assert_eq!(SECOND_CLOCK.with(|c| c.now()), Some(9));
        let other = std::thread::spawn(|| CLOCK.with(|c| c.now()))
            .join()
            .unwrap();
        assert_eq!(other, None);
    }

    #[test]
    fn a_stale_guard_leaves_a_later_installation_alone() {
        static RESET: DecideSeam =
            Seam::decide("test.reset", Op::Mutation, &[Effect::Fail], Global::new());
        let stale = RESET.fire_always();
        RESET.clear();
        let _live = RESET.fire_once_at(1);
        drop(stale);
        assert_eq!(RESET.crossed(), Decision::Fire(Effect::Fail));
    }

    #[test]
    #[should_panic(expected = "already installed")]
    fn installing_over_a_live_guard_is_refused() {
        static TWICE: DecideSeam =
            Seam::decide("test.twice", Op::Mutation, &[Effect::Fail], Global::new());
        let _first = TWICE.fire_always();
        let _second = TWICE.fire_once_at(2);
    }

    #[rustfmt::skip]
    static LOCATED: DecideSeam = Seam::decide("test.located", Op::Mutation, &[Effect::Fail], Global::new());
    const LINE_AFTER_LOCATED: u32 = line!();

    #[test]
    #[rustfmt::skip]
    fn a_seam_records_its_declaration_and_where_it_last_fired() {
        assert!(LOCATED.site().file().ends_with("lib.rs"));
        assert_eq!(LOCATED.site().line(), LINE_AFTER_LOCATED - 1);
        assert_eq!(LOCATED.crossed(), Decision::Pass);
        assert!(LOCATED.last_fired().is_none(), "a passing crossing records nothing");
        let guard = LOCATED.fire_once_at(1);
        let _ = LOCATED.crossed(); let fired_line = line!();
        let _ = LOCATED.crossed();
        drop(guard);
        let fired = LOCATED.last_fired().unwrap();
        assert_eq!((fired.file(), fired.line()), (LOCATED.site().file(), fired_line), "a later passing crossing does not move it");
        let entry: &dyn SeamEntry = &LOCATED;
        assert_eq!(entry.site().line(), LINE_AFTER_LOCATED - 1);
    }

    #[test]
    fn catalog_row_reads_the_seam() {
        let entry: &dyn SeamEntry = &ROW;
        assert_eq!(entry.name(), "test.row");
        assert_eq!(entry.effects(), &[Effect::Fail]);
        let seam = entry.as_decide().unwrap();
        let _guard = seam.fire_always();
        assert_eq!(ROW.crossed(), Decision::Fire(Effect::Fail));
        entry.clear();
        assert_eq!(ROW.crossed(), Decision::Pass);
    }
}
