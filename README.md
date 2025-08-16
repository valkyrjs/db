<p align="center">
  <img src="https://user-images.githubusercontent.com/1998130/229430454-ca0f2811-d874-4314-b13d-c558de8eec7e.svg" />
</p>

# Valkyr Database

Attempts to provide a practical data storage solution that utilizes MongoDB syntax for client-side read and write operations. Designed to be framework-agnostic, it can be easily integrated with any framework or even used without one.

The database was developed to overcome limitations in storing substantial amounts of data on the client side, which is not feasible with traditional storage solutions like localStorage. Instead, Valkyr Database relies on configurable database adapters such as in memory and indexeddb for browsers and async storage for hybrid mobile solutions, offering a larger storage capacity.

Additionally, the solution is tailored to provide native observability and effective management functionality, removing the reliance on client side state management utilities.
